# AMT

[English](README.md)

本仓库包含 AMT 的实现、用于评估吞吐量、存储和归档机制的以太坊有序状态访问回放，以及与原生 MPT 的逻辑状态一致性比较。代码基于 [go-ethereum](https://github.com/ethereum/go-ethereum)，并保留了原有的 Go 模块路径，以便修改后的 trie 包共同构建。

## 代码目录

- `trie/archive/mpt`：活跃树、档案桶、裁剪和复活。
- `trie/archive`：档案存储和有序状态访问回放。
- `core/tree_test`：轨迹导出程序和逻辑状态一致性比较。

## 随代码提供的轨迹样本

`trie/archive/testdata/trace_sample_100mb` 包含六个按访问顺序排列的压缩轨迹分片：总大小为 100,081,752 字节，共 4,687,270 条记录，覆盖区块 9,046,147 至 9,047,452。每个分片在完整区块的边界结束。`manifest.json` 记录了各分片的 SHA-256 校验值和区块范围。

该样本可用于检查真实轨迹的读取和短程回放。按照论文的完整回放参数，它不足以完成一轮归档。完整的以太坊主网交易数据集和轨迹数据仍需另行提供。

## 完整轨迹回放：吞吐量、存储与归档机制

回放按原始顺序处理以太坊主网前 1,000 万个区块中的状态访问。每个源区块对应一次提交，轨迹中没有记录的区块也照常推进。AMT 使用 19 位分片前缀，每个区块推进一个分片的裁剪。回放记录吞吐量、存储、活跃命中率、过滤器假阳性率、状态复活、分片裁剪和档案桶占用情况。提交前对该区块复活过的不同档案桶各验证一次。

下面的命令仅用于快速检查真实轨迹的输入与回放：从附带样本中执行两个区块，使用缩小的分片深度。在仓库根目录运行；`<fresh-output-dir>` 必须指向尚不存在的目录：

```sh
go test ./trie/archive -run '^TestAMTTraceStress$' -count=1 -timeout 10m -args -traceStressInputDir ./testdata/trace_sample_100mb -traceStressBaseDir <fresh-output-dir> -traceStressOps 0 -traceStressBlocks 2 -traceStressBatchByBlock=true -traceStressMPTVerifyAtCommit=true -traceStressActivateArchivedKeyOnRead=true -traceStressMetricsBatches 2 -traceStressShardDepthBits 1 -traceStressCompactPerWindow=false
```

运行论文中的 AMT 完整回放时，提供包含全部 `state_access_trace_*.csv.gz` 文件的目录：

```sh
go test ./trie/archive -run '^TestAMTTraceStress$' -count=1 -timeout 0 -args -traceStressInputDir <trace-dir> -traceStressBaseDir <fresh-output-dir> -traceStressOps 0 -traceStressEndBlock 10000000 -traceStressBatchByBlock=true -traceStressMPTVerifyAtCommit=true -traceStressMetricsBatches 25000 -traceStressShardDepthBits 19 -traceStressTrieBackend pathdb -traceStressActivateArchivedKeyOnRead=true
```

结果写入输出目录下的 `results/amt_trace_stress.csv` 和 `results/summary.json`。如需按区块范围精确截取，使用 `-traceStressOps=0`，并设置 `-traceStressEndBlock` 或 `-traceStressBlocks`。使用 `-traceStressPruneEveryBlocks` 时，需设置 `-traceStressPruneEveryBatches=0`。

## 与 MPT 的逻辑状态一致性验证

论文用 5,000 万次有序主网状态操作比较 AMT 与原生 MPT。12 位分片前缀使裁剪和状态复活更频繁。本测试比较回放期间返回的状态值，并在结束时比较全部逻辑键值对。输入可以是 `state_access_trace_*.csv` 或 `state_access_trace_*.csv.gz` 文件；结果写入一个新建输出目录中的 `result.json`：

```sh
go test ./core/tree_test -run '^TestE3StateSemanticEquivalence$' -count=1 -timeout 0 -args -e3InputDir <trace-dir> -e3OutputDir <fresh-output-dir> -e3Ops 50000000 -e3Depth 12
```

下文的合成轨迹快速测试无需外部数据，可检查归档与复活的状态语义。

## 生成轨迹

首先使用 [`ethereum-etl`](https://github.com/blockchain-etl/ethereum-etl)，通过以太坊节点的 RPC 接口导出区块和交易。例如，生成第一组文件：

```sh
ethereumetl export_blocks_and_transactions --start-block 0 --end-block 999999 --blocks-output <corpus>/blocks_1.csv --transactions-output <corpus>/transactions_1.csv --provider-uri <ethereum-rpc-url>
```

后续区块按不重叠的连续范围导出，文件依次命名为 `blocks_2.csv`、`transactions_2.csv` 等，并将每组同编号文件放在同一数据目录。轨迹导出程序按文件编号顺序读取 `transactions_*.csv`，并查找对应的 `blocks_*.csv`。交易 CSV 的前 11 列须依次为 `hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input`。区块 CSV 应包含 `number`、`timestamp` 和 `miner` 列。

准备好上述文件后，通过 `TestExportStateAccessTrace` 导出有序状态访问轨迹：

```sh
go test ./core/tree_test -run '^TestExportStateAccessTrace$' -count=1 -timeout 0 -args -traceDataDir <corpus> -traceOutDir <trace-dir> -traceStartFile 1 -traceEndFile <last-file>
```

`-traceEndFile 0` 表示处理所有交易文件。`-traceMaxBlocks N` 可限制导出的区块数；设为 0 时处理选中的所有文件。导出程序默认使用内存 KV 数据库。设置 `-traceUseMemory=false` 后，数据库将写入 `<trace-dir>/trace_state_db`。输出分片保存的是值的哈希。

## 本地检查

安装 Go 1.26 或更新版本后运行：

```sh
go test ./trie/archive/... -count=1
go test ./core/tree_test -run '^TestE3SemanticTraceSmoke$' -count=1
go build ./...
```

复现论文中的性能结果需要完整的外部数据集，以及各图表对应的实验参数。
