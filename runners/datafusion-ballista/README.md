# SQLBench Ballista Runner

## Build

```bash
cargo build --release
```

## Run Single Query

```bash
./target/release/sqlbench-ballista \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=10/ \
  --iterations 1 \
  --output . \
  --query 16
```

## Run All Queries

```bash
./target/release/sqlbench-ballista \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=10/ \
  --iterations 1 \
  --output . \
  --num-queries 22
```
