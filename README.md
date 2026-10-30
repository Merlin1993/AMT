# AMT

[中文版](README.zh-CN.md)

This repository contains the AMT implementation, the ordered Ethereum state-access replay used to measure throughput, storage, and archival mechanisms, and a logical-state consistency comparison against the native MPT. It is based on [go-ethereum](https://github.com/ethereum/go-ethereum) and retains its Go module path so the modified trie packages build together.

## Code map

- `trie/archive/mpt`: active tree, archive buckets, pruning, and resurrection.
- `trie/archive`: archive storage and ordered state-access replay.
- `core/tree_test`: trace exporter and logical-state consistency comparison.

## Bundled trace sample

`trie/archive/testdata/trace_sample_100mb` contains four compressed, ordered trace shards totaling 100,083,833 bytes and 4,687,270 records from blocks 9,046,147–9,047,452. Each shard ends at a block boundary. Its `manifest.json` records their SHA-256 checksums and ranges.

The sample can check the real trace input and a short replay. It is too short to complete an archive round with the paper's full-replay settings. The full mainnet transaction dataset and trace corpus are external.

## Full-trace replay: throughput, storage, and archival mechanisms

The replay processes ordered state accesses from the first 10 million Ethereum mainnet blocks. Each source block forms one commit batch, including blocks without trace records. AMT uses a 19-bit shard prefix and advances pruning by one shard per block. The replay records throughput, storage, active hits, filter false positives, resurrection, pruning, and bucket occupancy. Commit-time verification audits each distinct revived bucket once per block.

For a quick input-path check, replay two blocks from the bundled sample. This uses a reduced shard depth. Run from the repository root; `<fresh-output-dir>` must name a directory that does not exist:

```sh
go test ./trie/archive -run '^TestAMTTraceStress$' -count=1 -timeout 10m -args -traceStressInputDir ./testdata/trace_sample_100mb -traceStressBaseDir <fresh-output-dir> -traceStressOps 0 -traceStressBlocks 2 -traceStressBatchByBlock=true -traceStressMPTVerifyAtCommit=true -traceStressActivateArchivedKeyOnRead=true -traceStressMetricsBatches 2 -traceStressShardDepthBits 1 -traceStressCompactPerWindow=false
```

For the paper's AMT replay, supply the complete directory of `state_access_trace_*.csv.gz` files:

```sh
go test ./trie/archive -run '^TestAMTTraceStress$' -count=1 -timeout 0 -args -traceStressInputDir <trace-dir> -traceStressBaseDir <fresh-output-dir> -traceStressOps 0 -traceStressEndBlock 10000000 -traceStressBatchByBlock=true -traceStressMPTVerifyAtCommit=true -traceStressMetricsBatches 25000 -traceStressShardDepthBits 19 -traceStressTrieBackend pathdb -traceStressActivateArchivedKeyOnRead=true
```

Results are written to `results/amt_trace_stress.csv` and `results/summary.json` beneath the output directory. Set `-traceStressOps=0` with `-traceStressEndBlock` or `-traceStressBlocks` to select an exact block range. Use `-traceStressPruneEveryBatches=0` when selecting `-traceStressPruneEveryBlocks`.

## Logical-state consistency against MPT

The paper compares AMT and the native MPT over 50 million ordered mainnet state operations. A 12-bit shard prefix triggers more frequent pruning and resurrection. This test compares returned values during replay and all logical key–value pairs at the end. It accepts `state_access_trace_*.csv` and `state_access_trace_*.csv.gz` files and writes `result.json` in a fresh output directory:

```sh
go test ./core/tree_test -run '^TestE3StateSemanticEquivalence$' -count=1 -timeout 0 -args -e3InputDir <trace-dir> -e3OutputDir <fresh-output-dir> -e3Ops 50000000 -e3Depth 12
```

The synthetic smoke test below checks archive and resurrection semantics without external data.

## Generate a trace

First export blocks and transactions from an Ethereum node's RPC endpoint with [`ethereum-etl`](https://github.com/blockchain-etl/ethereum-etl). For example, to create the first pair of files:

```sh
ethereumetl export_blocks_and_transactions --start-block 0 --end-block 999999 --blocks-output <corpus>/blocks_1.csv --transactions-output <corpus>/transactions_1.csv --provider-uri <ethereum-rpc-url>
```

Export subsequent, nonoverlapping block ranges into `blocks_2.csv` and `transactions_2.csv`, and so on. Keep each numbered pair in the same corpus directory. The trace exporter reads `transactions_*.csv` in numeric file order and looks up the matching `blocks_*.csv`. Transaction columns must start with `hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input` in that order. Block files should include `number`, `timestamp`, and `miner` columns.

Then export ordered state accesses through `TestExportStateAccessTrace`:

```sh
go test ./core/tree_test -run '^TestExportStateAccessTrace$' -count=1 -timeout 0 -args -traceDataDir <corpus> -traceOutDir <trace-dir> -traceStartFile 1 -traceEndFile <last-file>
```

`-traceEndFile 0` selects every transaction file. `-traceMaxBlocks N` limits the exported blocks; zero processes all selected files. The exporter uses an in-memory KV database by default. Set `-traceUseMemory=false` to use `<trace-dir>/trace_state_db` on disk. Output shards contain value hashes.

## Local checks

With Go 1.26 or newer installed, run:

```sh
go test ./trie/archive/... -count=1
go test ./core/tree_test -run '^TestE3SemanticTraceSmoke$' -count=1
go build ./...
```

Reproducing the paper's performance results requires the full external corpus and the settings for each reported figure.
