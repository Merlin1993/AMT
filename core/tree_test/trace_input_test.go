package tree

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

type txIndex struct {
	blockNum uint64
	offset   int64
}

type TransactionStreamer struct {
	file    *os.File
	indices []txIndex
	curr    int
}

func newTransactionStreamer(filePath string) (*TransactionStreamer, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	var indices []txIndex

	bufr := bufio.NewReader(f)

	_, _, err = bufr.ReadLine()
	if err != nil {
		f.Close()
		return nil, err
	}

	offset, _ := f.Seek(0, os.SEEK_CUR)
	offset -= int64(bufr.Buffered())

	for {
		lineOffset := offset
		line, err := bufr.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			break
		}
		offset += int64(len(line))

		sline := string(line)
		if len(sline) < 10 || strings.HasPrefix(sline, "hash") {
			continue
		}

		parts := strings.SplitN(sline, ",", 5)
		if len(parts) < 4 {
			continue
		}
		blockNum, _ := strconv.ParseUint(parts[3], 10, 64)
		indices = append(indices, txIndex{blockNum: blockNum, offset: lineOffset})
	}

	sort.Slice(indices, func(i, j int) bool {
		if indices[i].blockNum == indices[j].blockNum {
			return indices[i].offset < indices[j].offset
		}
		return indices[i].blockNum < indices[j].blockNum
	})

	return &TransactionStreamer{
		file:    f,
		indices: indices,
		curr:    0,
	}, nil
}

func (s *TransactionStreamer) PeekBlockNum() (uint64, bool) {
	if s.curr >= len(s.indices) {
		return 0, false
	}
	return s.indices[s.curr].blockNum, true
}

func (s *TransactionStreamer) PopBlock(targetBlock uint64) ([]*core.Message, bool) {
	if s.curr >= len(s.indices) || s.indices[s.curr].blockNum != targetBlock {
		return nil, false
	}

	var msgs []*core.Message
	bufr := bufio.NewReader(s.file)

	for s.curr < len(s.indices) && s.indices[s.curr].blockNum == targetBlock {
		_, err := s.file.Seek(s.indices[s.curr].offset, os.SEEK_SET)
		if err != nil {
			s.curr++
			continue
		}
		bufr.Reset(s.file)
		line, _, err := bufr.ReadLine()
		if err == nil {
			reader := csv.NewReader(strings.NewReader(string(line)))
			record, err := reader.Read()
			if err == nil {
				msg, err := parseCSVRecordToMessage(record)
				if err == nil {
					msgs = append(msgs, msg)
				}
			}
		}
		s.curr++
	}

	return msgs, len(msgs) > 0
}

func (s *TransactionStreamer) Close() {
	if s.file != nil {
		s.file.Close()
	}
}

func parseCSVRecordToMessage(record []string) (*core.Message, error) {
	if len(record) < 10 {
		return nil, fmt.Errorf("invalid record length")
	}

	from := common.HexToAddress(record[5])
	var to *common.Address
	if record[6] != "" && record[6] != "null" {
		toAddr := common.HexToAddress(record[6])
		to = &toAddr
	}

	value := new(big.Int)
	value.SetString(record[7], 10)

	gasLimit, _ := strconv.ParseUint(record[8], 10, 64)
	if gasLimit == 0 {
		gasLimit = 21000
	}

	gasPrice := new(big.Int)
	gasPrice.SetString(record[9], 10)
	if gasPrice.Sign() == 0 {
		gasPrice = big.NewInt(1000000000)
	}

	nonce, _ := strconv.ParseUint(record[1], 10, 64)
	data := common.FromHex(record[10])

	return &core.Message{
		To:               to,
		From:             from,
		Nonce:            nonce,
		Value:            value,
		GasLimit:         gasLimit,
		GasPrice:         gasPrice,
		GasFeeCap:        gasPrice,
		GasTipCap:        gasPrice,
		Data:             data,
		SkipNonceChecks:  true,
		SkipFromEOACheck: true,
	}, nil
}

// 按文件名中的数字序号排序轨迹分片。
func sortTraceFilesNaturally(files []string) {
	sort.Slice(files, func(i, j int) bool {
		fileNameI := filepath.Base(files[i])
		fileNameJ := filepath.Base(files[j])

		numStrI := ""
		numStrJ := ""

		if idx := strings.Index(fileNameI, "transactions_"); idx >= 0 {
			numStrI = fileNameI[idx+len("transactions_"):]
		} else if idx := strings.Index(fileNameI, "blocks_"); idx >= 0 {
			numStrI = fileNameI[idx+len("blocks_"):]
		}
		if idx := strings.Index(fileNameJ, "transactions_"); idx >= 0 {
			numStrJ = fileNameJ[idx+len("transactions_"):]
		} else if idx := strings.Index(fileNameJ, "blocks_"); idx >= 0 {
			numStrJ = fileNameJ[idx+len("blocks_"):]
		}

		numStrI = strings.TrimSuffix(numStrI, ".csv")
		numStrJ = strings.TrimSuffix(numStrJ, ".csv")

		if numStrI == "" || numStrJ == "" {
			return files[i] < files[j]
		}

		numI, errI := strconv.Atoi(numStrI)
		numJ, errJ := strconv.Atoi(numStrJ)

		if errI != nil || errJ != nil {
			return files[i] < files[j]
		}

		return numI < numJ
	})
}

var traceBlockTimestamps = make(map[uint64]uint64)

var traceBlockMiners = make(map[uint64]common.Address)

func loadTraceBlockMetadata(dataDir string, fileIndex string) error {
	// 清空旧数据，避免跨文件累积导致内存过高
	traceBlockTimestamps = make(map[uint64]uint64)
	traceBlockMiners = make(map[uint64]common.Address)

	var blockFile string
	if fileIndex == "" {
		blockFile = filepath.Join(dataDir, "blocks.csv")
	} else {
		blockFile = filepath.Join(dataDir, fmt.Sprintf("blocks_%s.csv", fileIndex))
	}

	if _, err := os.Stat(blockFile); os.IsNotExist(err) {
		return fmt.Errorf("区块文件不存在: %s", blockFile)
	}

	csvFile, err := os.Open(blockFile)
	if err != nil {
		return fmt.Errorf("无法打开区块CSV文件 %s: %v", blockFile, err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("读取区块CSV头失败: %v", err)
	}

	var numberIdx, timestampIdx, minerIdx int = -1, -1, -1
	for i, header := range headers {
		switch strings.ToLower(header) {
		case "number":
			numberIdx = i
		case "timestamp":
			timestampIdx = i
		case "miner", "author", "coinbase":
			minerIdx = i
		}
	}

	if numberIdx == -1 {
		return fmt.Errorf("区块CSV文件 %s 缺少number字段", blockFile)
	}

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		num, err := strconv.ParseUint(record[numberIdx], 10, 64)
		if err != nil {
			continue
		}

		if timestampIdx != -1 {
			timestamp, err := strconv.ParseUint(record[timestampIdx], 10, 64)
			if err == nil {
				traceBlockTimestamps[num] = timestamp
			}
		}

		if minerIdx != -1 && record[minerIdx] != "" && record[minerIdx] != "null" {
			traceBlockMiners[num] = common.HexToAddress(record[minerIdx])
		}
	}

	return nil
}

func traceFileIndex(filePath string) string {
	fileName := filepath.Base(filePath)

	if !strings.Contains(fileName, "_") {
		return ""
	}

	parts := strings.Split(fileName, "_")
	if len(parts) < 2 {
		return ""
	}

	return strings.TrimSuffix(parts[1], ".csv")
}

func findTraceTransactionFiles(dataDir string) ([]string, error) {
	pattern := filepath.Join(dataDir, "transactions_*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("查找CSV文件失败: %v", err)
	}

	sortTraceFilesNaturally(files)
	return files, nil
}
