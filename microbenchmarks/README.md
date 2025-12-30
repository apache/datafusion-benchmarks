<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Microbenchmarks

This directory contains microbenchmarks for comparing DataFusion and DuckDB performance on individual SQL functions. Unlike the TPC-H and TPC-DS benchmarks which test full query execution, these microbenchmarks focus on the performance of specific SQL functions and expressions.

## Overview

The benchmarks generate synthetic data, write it to Parquet format, and then measure the execution time of various SQL functions across both DataFusion and DuckDB. Results include per-function timing comparisons and summary statistics.

## Setup

Create a virtual environment and install dependencies:

```shell
cd microbenchmarks
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run a benchmark:

```shell
python microbenchmarks.py
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--rows` | `1000000` | Number of rows in the generated test data |
| `--warmup` | `2` | Number of warmup iterations before timing |
| `--iterations` | `5` | Number of timed iterations (results are averaged) |
| `--output` | stdout | Output file path for markdown results |

### Examples

Run the benchmark with default settings:

```shell
python microbenchmark.py
```

Run the benchmark with 10 million rows:

```shell
python microbenchmarks.py --rows 10000000
```

Run the benchmark and save results to a file:

```shell
python microbenchmarks.py --output results.md
```

## Output

The benchmark outputs a markdown table comparing execution times:

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| trim | 12.34 | 15.67 | 1.27x | DataFusion |
| lower | 8.90 | 7.50 | 1.19x | DuckDB |
| ... | ... | ... | ... | ... |

A summary section shows overall statistics including how many functions each engine won and total execution times.