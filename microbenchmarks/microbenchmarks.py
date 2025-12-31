#!/usr/bin/env python3
"""
Microbenchmark comparing DataFusion and DuckDB performance
for SQL string functions on Parquet files.
"""

import tempfile
import time
import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import datafusion
import duckdb


@dataclass
class BenchmarkResult:
    """Stores benchmark results for a single function."""
    function_name: str
    datafusion_time_ms: float
    duckdb_time_ms: float
    rows: int

    @property
    def speedup(self) -> float:
        """DuckDB time / DataFusion time (>1 means DataFusion is faster)."""
        if self.datafusion_time_ms == 0:
            return float('inf')
        return self.duckdb_time_ms / self.datafusion_time_ms


@dataclass
class StringFunction:
    """Defines a string function with syntax for both engines."""
    name: str
    datafusion_expr: str  # Expression using {col} as placeholder for column name
    duckdb_expr: str      # Expression using {col} as placeholder for column name


# String functions to benchmark
# {col} will be replaced with the actual column name
STRING_FUNCTIONS = [
    StringFunction("trim", "trim({col})", "trim({col})"),
    StringFunction("ltrim", "ltrim({col})", "ltrim({col})"),
    StringFunction("rtrim", "rtrim({col})", "rtrim({col})"),
    StringFunction("lower", "lower({col})", "lower({col})"),
    StringFunction("upper", "upper({col})", "upper({col})"),
    StringFunction("length", "length({col})", "length({col})"),
    StringFunction("char_length", "char_length({col})", "length({col})"),
    StringFunction("reverse", "reverse({col})", "reverse({col})"),
    StringFunction("repeat_3", "repeat({col}, 3)", "repeat({col}, 3)"),
    StringFunction("concat", "concat({col}, {col})", "concat({col}, {col})"),
    StringFunction("concat_ws", "concat_ws('-', {col}, {col})", "concat_ws('-', {col}, {col})"),
    StringFunction("substring_1_5", "substring({col}, 1, 5)", "substring({col}, 1, 5)"),
    StringFunction("left_5", "left({col}, 5)", "left({col}, 5)"),
    StringFunction("right_5", "right({col}, 5)", "right({col}, 5)"),
    StringFunction("lpad_20", "lpad({col}, 20, '*')", "lpad({col}, 20, '*')"),
    StringFunction("rpad_20", "rpad({col}, 20, '*')", "rpad({col}, 20, '*')"),
    StringFunction("replace", "replace({col}, 'a', 'X')", "replace({col}, 'a', 'X')"),
    StringFunction("translate", "translate({col}, 'aeiou', '12345')", "translate({col}, 'aeiou', '12345')"),
    StringFunction("ascii", "ascii({col})", "ascii({col})"),
    StringFunction("md5", "md5({col})", "md5({col})"),
    StringFunction("sha256", "sha256({col})", "sha256({col})"),
    StringFunction("btrim", "btrim({col}, ' ')", "trim({col}, ' ')"),
    StringFunction("split_part", "split_part({col}, ' ', 1)", "split_part({col}, ' ', 1)"),
    StringFunction("starts_with", "starts_with({col}, 'test')", "starts_with({col}, 'test')"),
    StringFunction("ends_with", "ends_with({col}, 'data')", "ends_with({col}, 'data')"),
    StringFunction("strpos", "strpos({col}, 'e')", "strpos({col}, 'e')"),
    StringFunction("regexp_replace", "regexp_replace({col}, '[aeiou]', '*')", "regexp_replace({col}, '[aeiou]', '*', 'g')"),
]


def generate_test_data(num_rows: int = 1_000_000, use_string_view: bool = False) -> pa.Table:
    """Generate test data with various string patterns."""
    import random
    import string

    random.seed(42)  # For reproducibility

    # Generate diverse string data
    strings = []
    for i in range(num_rows):
        # Mix of different string patterns
        pattern_type = i % 5
        if pattern_type == 0:
            # Short strings with spaces
            s = f"  test_{i % 1000}  "
        elif pattern_type == 1:
            # Longer strings
            s = ''.join(random.choices(string.ascii_lowercase, k=20))
        elif pattern_type == 2:
            # Mixed case with numbers
            s = f"TestData_{i}_Value"
        elif pattern_type == 3:
            # Strings with special patterns
            s = f"hello world {i % 100} data"
        else:
            # Random length strings
            length = random.randint(5, 50)
            s = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
        strings.append(s)

    str_type = pa.string_view() if use_string_view else pa.string()
    table = pa.table({
        'str_col': pa.array(strings, type=str_type)
    })

    return table


def setup_datafusion(parquet_path: str) -> datafusion.SessionContext:
    """Create and configure DataFusion context with single thread/partition."""
    config = datafusion.SessionConfig().with_target_partitions(1)
    ctx = datafusion.SessionContext(config)
    ctx.register_parquet('test_data', parquet_path)
    return ctx


def setup_duckdb(parquet_path: str) -> duckdb.DuckDBPyConnection:
    """Create and configure DuckDB connection with single thread."""
    conn = duckdb.connect(':memory:', config={'threads': 1})
    conn.execute(f"CREATE VIEW test_data AS SELECT * FROM read_parquet('{parquet_path}')")
    return conn


def benchmark_datafusion(ctx: datafusion.SessionContext, expr: str,
                         warmup: int = 2, iterations: int = 5) -> float:
    """Benchmark a query in DataFusion, return average time in ms."""
    query = f"SELECT {expr} FROM test_data"

    # Warmup runs
    for _ in range(warmup):
        ctx.sql(query).collect()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        ctx.sql(query).collect()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms

    return sum(times) / len(times)


def benchmark_duckdb(conn: duckdb.DuckDBPyConnection, expr: str,
                     warmup: int = 2, iterations: int = 5) -> float:
    """Benchmark a query in DuckDB, return average time in ms."""
    query = f"SELECT {expr} FROM test_data"

    # Use fetch_arrow_table() for fair comparison with DataFusion's collect()
    # Both return Arrow data without Python object conversion overhead
    for _ in range(warmup):
        conn.execute(query).fetch_arrow_table()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        conn.execute(query).fetch_arrow_table()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms

    return sum(times) / len(times)


def run_benchmarks(num_rows: int = 1_000_000,
                   warmup: int = 2,
                   iterations: int = 5,
                   use_string_view: bool = False) -> list[BenchmarkResult]:
    """Run all benchmarks and return results."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, 'test_data.parquet')

        # Generate and save test data
        str_type = "StringView" if use_string_view else "String"
        print(f"Generating {num_rows:,} rows of test data (type: {str_type})...")
        table = generate_test_data(num_rows, use_string_view)
        pq.write_table(table, parquet_path)
        print(f"Parquet file written to: {parquet_path}")
        print(f"File size: {os.path.getsize(parquet_path) / 1024 / 1024:.2f} MB")

        # Setup engines
        print("\nSetting up DataFusion...")
        df_ctx = setup_datafusion(parquet_path)

        print("Setting up DuckDB...")
        duck_conn = setup_duckdb(parquet_path)

        # Run benchmarks
        print(f"\nRunning benchmarks ({warmup} warmup, {iterations} iterations each)...\n")

        col = 'str_col'
        for func in STRING_FUNCTIONS:
            df_expr = func.datafusion_expr.format(col=col)
            duck_expr = func.duckdb_expr.format(col=col)

            print(f"  Benchmarking: {func.name}...", end=" ", flush=True)

            try:
                df_time = benchmark_datafusion(df_ctx, df_expr, warmup, iterations)
            except Exception as e:
                print(f"DataFusion error: {e}")
                df_time = float('nan')

            try:
                duck_time = benchmark_duckdb(duck_conn, duck_expr, warmup, iterations)
            except Exception as e:
                print(f"DuckDB error: {e}")
                duck_time = float('nan')

            result = BenchmarkResult(
                function_name=func.name,
                datafusion_time_ms=df_time,
                duckdb_time_ms=duck_time,
                rows=num_rows
            )
            results.append(result)

            # Print progress
            if df_time == df_time and duck_time == duck_time:  # Check for NaN
                faster = "DataFusion" if df_time < duck_time else "DuckDB"
                ratio = max(df_time, duck_time) / min(df_time, duck_time)
                print(f"done ({faster} {ratio:.2f}x faster)")
            else:
                print("done (with errors)")

        duck_conn.close()

    return results


def format_results_markdown(results: list[BenchmarkResult], use_string_view: bool = False) -> str:
    """Format benchmark results as a markdown table."""
    str_type = "StringView" if use_string_view else "String"
    lines = [
        "# String Function Microbenchmarks: DataFusion vs DuckDB",
        "",
        f"**DataFusion version:** {datafusion.__version__}  ",
        f"**DuckDB version:** {duckdb.__version__}  ",
        f"**Rows:** {results[0].rows:,}  ",
        f"**String type:** {str_type}  ",
        "**Configuration:** Single thread, single partition",
        "",
        f"| Function | DataFusion {datafusion.__version__} (ms) | DuckDB {duckdb.__version__} (ms) | Speedup | Faster |",
        "|----------|----------------:|------------:|--------:|--------|",
    ]

    for r in results:
        if r.datafusion_time_ms != r.datafusion_time_ms or r.duckdb_time_ms != r.duckdb_time_ms:
            # Handle NaN
            lines.append(f"| {r.function_name} | ERROR | ERROR | N/A | N/A |")
        else:
            speedup = r.speedup
            if speedup > 1:
                faster = "DataFusion"
                speedup_str = f"{speedup:.2f}x"
            else:
                faster = "DuckDB"
                speedup_str = f"{1/speedup:.2f}x"

            lines.append(
                f"| {r.function_name} | {r.datafusion_time_ms:.2f} | "
                f"{r.duckdb_time_ms:.2f} | {speedup_str} | {faster} |"
            )

    # Summary statistics
    valid_results = [r for r in results
                     if r.datafusion_time_ms == r.datafusion_time_ms
                     and r.duckdb_time_ms == r.duckdb_time_ms]

    if valid_results:
        df_wins = sum(1 for r in valid_results if r.speedup > 1)
        duck_wins = len(valid_results) - df_wins

        df_total = sum(r.datafusion_time_ms for r in valid_results)
        duck_total = sum(r.duckdb_time_ms for r in valid_results)

        lines.extend([
            "",
            "## Summary",
            "",
            f"- **Functions tested:** {len(valid_results)}",
            f"- **DataFusion faster:** {df_wins} functions",
            f"- **DuckDB faster:** {duck_wins} functions",
            f"- **Total DataFusion time:** {df_total:.2f} ms",
            f"- **Total DuckDB time:** {duck_total:.2f} ms",
        ])

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Benchmark string functions: DataFusion vs DuckDB"
    )
    parser.add_argument(
        "--rows", type=int, default=1_000_000,
        help="Number of rows in test data (default: 1,000,000)"
    )
    parser.add_argument(
        "--warmup", type=int, default=2,
        help="Number of warmup iterations (default: 2)"
    )
    parser.add_argument(
        "--iterations", type=int, default=5,
        help="Number of timed iterations (default: 5)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output file for markdown results (default: stdout)"
    )
    parser.add_argument(
        "--string-view", action="store_true",
        help="Use StringView type instead of String (default: False)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("String Function Microbenchmarks: DataFusion vs DuckDB")
    print("=" * 60)

    results = run_benchmarks(
        num_rows=args.rows,
        warmup=args.warmup,
        iterations=args.iterations,
        use_string_view=args.string_view
    )

    markdown = format_results_markdown(results, use_string_view=args.string_view)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60 + "\n")
    print(markdown)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(markdown)
        print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
