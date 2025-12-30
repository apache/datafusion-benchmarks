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

## Benchmark Suites

Three benchmark suites are available:

- **string**: String manipulation functions (trim, lower, upper, concat, substring, regex, etc.)
- **temporal**: Date/time functions (extract, date_trunc, date_part, interval arithmetic, formatting)
- **conditional**: Conditional expressions (CASE WHEN, COALESCE, NULLIF, GREATEST/LEAST)

## Setup

Create a virtual environment and install dependencies:

```shell
cd microbenchmarks
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run a benchmark suite:

```shell
python string_functions_benchmark.py --suite <suite_name>
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--suite` | `string` | Benchmark suite to run: `string`, `temporal`, or `conditional` |
| `--rows` | `1000000` | Number of rows in the generated test data |
| `--warmup` | `2` | Number of warmup iterations before timing |
| `--iterations` | `5` | Number of timed iterations (results are averaged) |
| `--output` | stdout | Output file path for markdown results |

### Examples

Run the string functions benchmark with default settings:

```shell
python string_functions_benchmark.py
```

Run the temporal functions benchmark with 10 million rows:

```shell
python string_functions_benchmark.py --suite temporal --rows 10000000
```

Run the conditional expressions benchmark and save results to a file:

```shell
python string_functions_benchmark.py --suite conditional --output results.md
```

## Output

The benchmark outputs a markdown table comparing execution times:

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| trim | 12.34 | 15.67 | 1.27x | DataFusion |
| lower | 8.90 | 7.50 | 1.19x | DuckDB |
| ... | ... | ... | ... | ... |

A summary section shows overall statistics including how many functions each engine won and total execution times.

## Adding New Benchmarks

To add new functions to an existing suite, add a `BenchmarkFunction` entry to the appropriate list in `string_functions_benchmark.py`:

```python
BenchmarkFunction(
    "function_name",
    "datafusion_sql_expression({col})",
    "duckdb_sql_expression({col})"
)
```

The placeholders (e.g., `{col}`, `{str_col}`, `{ts_col}`) are replaced with actual column names at runtime.
