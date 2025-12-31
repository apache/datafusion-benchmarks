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

Benchmarks are organized into **suites**, each focusing on a specific category of SQL functions:

| Suite | Description | Functions |
|-------|-------------|-----------|
| `strings` | String manipulation functions (trim, lower, upper, concat, etc.) | 27 |
| `temporal` | Date/time functions (year, month, date_trunc, etc.) | 21 |
| `numeric` | Math functions (sqrt, pow, sin, cos, log, round, etc.) | 38 |

All benchmarks run in single-threaded mode for fair comparison between engines.

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
python microbenchmarks.py --suite strings
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--suite` | `strings` | Benchmark suite to run (`strings`, `temporal`, `numeric`) |
| `--rows` | `1000000` | Number of rows in the generated test data |
| `--warmup` | `2` | Number of warmup iterations before timing |
| `--iterations` | `5` | Number of timed iterations (results are averaged) |
| `--output` | stdout | Output file path for markdown results |
| `--string-view` | `false` | Use Arrow StringView type instead of String |

### Examples

Run the string functions benchmark (default):

```shell
python microbenchmarks.py
```

Run the temporal functions benchmark:

```shell
python microbenchmarks.py --suite temporal
```

Run with 10 million rows:

```shell
python microbenchmarks.py --suite strings --rows 10000000
```

Run with StringView type and save results:

```shell
python microbenchmarks.py --suite strings --string-view --output results.md
```

## Output

The benchmark outputs a markdown table comparing execution times:

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| trim | 12.34 | 15.67 | 1.27x | DataFusion |
| lower | 8.90 | 7.50 | 1.19x | DuckDB |
| ... | ... | ... | ... | ... |

A summary section shows overall statistics including how many functions each engine won and total execution times.

## Project Structure

```
microbenchmarks/
├── microbenchmarks.py      # Main benchmark runner
├── requirements.txt        # Python dependencies
└── suites/                 # Benchmark suite definitions
    ├── __init__.py         # Suite registry and base classes
    ├── strings.py          # String function benchmarks
    └── temporal.py         # Date/time function benchmarks
```

## Adding New Suites

To add a new benchmark suite:

1. Create a new file in `suites/` (e.g., `suites/numeric.py`)

2. Define your functions and data generator:

```python
from . import BenchmarkFunction, Suite
import pyarrow as pa

FUNCTIONS = [
    BenchmarkFunction("abs", "abs({col})", "abs({col})"),
    BenchmarkFunction("sqrt", "sqrt({col})", "sqrt({col})"),
    # ... more functions
]

def generate_data(num_rows: int, use_string_view: bool = False) -> pa.Table:
    # Generate appropriate test data
    return pa.table({'num_col': pa.array(...)})

SUITE = Suite(
    name="numeric",
    description="Numeric function benchmarks",
    column_name="num_col",
    functions=FUNCTIONS,
    generate_data=generate_data,
)
```

3. Register the suite in `suites/__init__.py`:

```python
from . import numeric

SUITES: dict[str, Suite] = {
    'strings': strings.SUITE,
    'temporal': temporal.SUITE,
    'numeric': numeric.SUITE,  # Add new suite here
}
```