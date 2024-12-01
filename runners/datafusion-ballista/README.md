# DataFusion Benchmarks: Ballista Runner

## Build

```bash
cargo build --release
```

## Run Single Query

```bash
./target/release/ballista-tpcbench \
  --concurrency 8 \
  --data-path /mnt/bigdata/tpch/sf100/ \
  --query-path ../../tpch/queries/ \
  --iterations 1 \
  --output . \
  --query 16
```

## Run All Queries

```bash
./target/release/ballista-tpcbench \
  --concurrency 8 \
  --data-path /mnt/bigdata/tpch/sf100/ \
  --query-path ../../tpch/queries/ \
  --iterations 1 \
  --output . \
  --num-queries 22
```
