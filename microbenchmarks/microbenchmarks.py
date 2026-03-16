#!/usr/bin/env python3
"""
Microbenchmark comparing DataFusion and DuckDB performance
for various SQL functions on Parquet files.
"""

import tempfile
import time
import os
from dataclasses import dataclass

import pyarrow.parquet as pq
import datafusion
import duckdb

from suites import get_suite, list_suites, Suite


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


def run_benchmarks(suite: Suite,
                   num_rows: int = 1_000_000,
                   warmup: int = 2,
                   iterations: int = 5,
                   use_string_view: bool = False) -> list[BenchmarkResult]:
    """Run all benchmarks for a suite and return results."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, 'test_data.parquet')

        # Generate and save test data
        print(f"Generating {num_rows:,} rows of test data for '{suite.name}' suite...")
        table = suite.generate_data(num_rows, use_string_view)
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

        col = suite.column_name
        for func in suite.functions:
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


def format_results_markdown(results: list[BenchmarkResult],
                            suite: Suite,
                            use_string_view: bool = False) -> str:
    """Format benchmark results as a markdown table."""
    str_type = "StringView" if use_string_view else "String"
    lines = [
        f"# {suite.description}: DataFusion vs DuckDB",
        "",
        f"**Suite:** {suite.name}  ",
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

    available_suites = list_suites()

    parser = argparse.ArgumentParser(
        description="Benchmark SQL functions: DataFusion vs DuckDB"
    )
    parser.add_argument(
        "--suite", type=str, default="strings",
        choices=available_suites,
        help=f"Benchmark suite to run (default: strings). Available: {', '.join(available_suites)}"
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

    suite = get_suite(args.suite)

    print("=" * 60)
    print(f"{suite.description}: DataFusion vs DuckDB")
    print("=" * 60)

    results = run_benchmarks(
        suite=suite,
        num_rows=args.rows,
        warmup=args.warmup,
        iterations=args.iterations,
        use_string_view=args.string_view
    )

    markdown = format_results_markdown(results, suite=suite, use_string_view=args.string_view)

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
