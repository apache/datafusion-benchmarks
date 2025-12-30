#!/usr/bin/env python3
"""
Microbenchmark comparing DataFusion and DuckDB performance
for SQL functions on Parquet files.

Supports multiple benchmark suites:
- string: String manipulation functions
- temporal: Date/time functions
- conditional: Conditional expressions (IF, CASE WHEN, COALESCE, etc.)
"""

import tempfile
import time
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
import random

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
class BenchmarkFunction:
    """Defines a function with syntax for both engines."""
    name: str
    datafusion_expr: str  # Expression using placeholders for column names
    duckdb_expr: str      # Expression using placeholders for column names


# =============================================================================
# STRING FUNCTIONS SUITE
# =============================================================================

STRING_FUNCTIONS = [
    BenchmarkFunction("trim", "trim({str_col})", "trim({str_col})"),
    BenchmarkFunction("ltrim", "ltrim({str_col})", "ltrim({str_col})"),
    BenchmarkFunction("rtrim", "rtrim({str_col})", "rtrim({str_col})"),
    BenchmarkFunction("lower", "lower({str_col})", "lower({str_col})"),
    BenchmarkFunction("upper", "upper({str_col})", "upper({str_col})"),
    BenchmarkFunction("length", "length({str_col})", "length({str_col})"),
    BenchmarkFunction("char_length", "char_length({str_col})", "length({str_col})"),
    BenchmarkFunction("reverse", "reverse({str_col})", "reverse({str_col})"),
    BenchmarkFunction("repeat_3", "repeat({str_col}, 3)", "repeat({str_col}, 3)"),
    BenchmarkFunction("concat", "concat({str_col}, {str_col})", "concat({str_col}, {str_col})"),
    BenchmarkFunction("concat_ws", "concat_ws('-', {str_col}, {str_col})", "concat_ws('-', {str_col}, {str_col})"),
    BenchmarkFunction("substring_1_5", "substring({str_col}, 1, 5)", "substring({str_col}, 1, 5)"),
    BenchmarkFunction("left_5", "left({str_col}, 5)", "left({str_col}, 5)"),
    BenchmarkFunction("right_5", "right({str_col}, 5)", "right({str_col}, 5)"),
    BenchmarkFunction("lpad_20", "lpad({str_col}, 20, '*')", "lpad({str_col}, 20, '*')"),
    BenchmarkFunction("rpad_20", "rpad({str_col}, 20, '*')", "rpad({str_col}, 20, '*')"),
    BenchmarkFunction("replace", "replace({str_col}, 'a', 'X')", "replace({str_col}, 'a', 'X')"),
    BenchmarkFunction("translate", "translate({str_col}, 'aeiou', '12345')", "translate({str_col}, 'aeiou', '12345')"),
    BenchmarkFunction("ascii", "ascii({str_col})", "ascii({str_col})"),
    BenchmarkFunction("md5", "md5({str_col})", "md5({str_col})"),
    BenchmarkFunction("sha256", "sha256({str_col})", "sha256({str_col})"),
    BenchmarkFunction("btrim", "btrim({str_col}, ' ')", "trim({str_col}, ' ')"),
    BenchmarkFunction("split_part", "split_part({str_col}, ' ', 1)", "split_part({str_col}, ' ', 1)"),
    BenchmarkFunction("starts_with", "starts_with({str_col}, 'test')", "starts_with({str_col}, 'test')"),
    BenchmarkFunction("ends_with", "ends_with({str_col}, 'data')", "ends_with({str_col}, 'data')"),
    BenchmarkFunction("strpos", "strpos({str_col}, 'e')", "strpos({str_col}, 'e')"),
    BenchmarkFunction("regexp_replace", "regexp_replace({str_col}, '[aeiou]', '*')", "regexp_replace({str_col}, '[aeiou]', '*', 'g')"),
]


def generate_string_data(num_rows: int) -> pa.Table:
    """Generate test data for string functions."""
    import string

    random.seed(42)

    strings = []
    for i in range(num_rows):
        pattern_type = i % 5
        if pattern_type == 0:
            s = f"  test_{i % 1000}  "
        elif pattern_type == 1:
            s = ''.join(random.choices(string.ascii_lowercase, k=20))
        elif pattern_type == 2:
            s = f"TestData_{i}_Value"
        elif pattern_type == 3:
            s = f"hello world {i % 100} data"
        else:
            length = random.randint(5, 50)
            s = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
        strings.append(s)

    return pa.table({'str_col': pa.array(strings, type=pa.string())})


# =============================================================================
# TEMPORAL FUNCTIONS SUITE
# =============================================================================

TEMPORAL_FUNCTIONS = [
    # Date extraction using EXTRACT (supported by both)
    BenchmarkFunction("extract_year", "extract(YEAR FROM {ts_col})", "extract(YEAR FROM {ts_col})"),
    BenchmarkFunction("extract_month", "extract(MONTH FROM {ts_col})", "extract(MONTH FROM {ts_col})"),
    BenchmarkFunction("extract_day", "extract(DAY FROM {ts_col})", "extract(DAY FROM {ts_col})"),
    BenchmarkFunction("extract_hour", "extract(HOUR FROM {ts_col})", "extract(HOUR FROM {ts_col})"),
    BenchmarkFunction("extract_minute", "extract(MINUTE FROM {ts_col})", "extract(MINUTE FROM {ts_col})"),
    BenchmarkFunction("extract_second", "extract(SECOND FROM {ts_col})", "extract(SECOND FROM {ts_col})"),
    BenchmarkFunction("extract_dow", "extract(DOW FROM {ts_col})", "extract(DOW FROM {ts_col})"),
    BenchmarkFunction("extract_doy", "extract(DOY FROM {ts_col})", "extract(DOY FROM {ts_col})"),
    BenchmarkFunction("extract_week", "extract(WEEK FROM {ts_col})", "extract(WEEK FROM {ts_col})"),
    BenchmarkFunction("extract_quarter", "extract(QUARTER FROM {ts_col})", "extract(QUARTER FROM {ts_col})"),
    BenchmarkFunction("extract_epoch", "extract(EPOCH FROM {ts_col})", "extract(EPOCH FROM {ts_col})"),

    # Date truncation
    BenchmarkFunction("date_trunc_year", "date_trunc('year', {ts_col})", "date_trunc('year', {ts_col})"),
    BenchmarkFunction("date_trunc_quarter", "date_trunc('quarter', {ts_col})", "date_trunc('quarter', {ts_col})"),
    BenchmarkFunction("date_trunc_month", "date_trunc('month', {ts_col})", "date_trunc('month', {ts_col})"),
    BenchmarkFunction("date_trunc_week", "date_trunc('week', {ts_col})", "date_trunc('week', {ts_col})"),
    BenchmarkFunction("date_trunc_day", "date_trunc('day', {ts_col})", "date_trunc('day', {ts_col})"),
    BenchmarkFunction("date_trunc_hour", "date_trunc('hour', {ts_col})", "date_trunc('hour', {ts_col})"),
    BenchmarkFunction("date_trunc_minute", "date_trunc('minute', {ts_col})", "date_trunc('minute', {ts_col})"),
    BenchmarkFunction("date_trunc_second", "date_trunc('second', {ts_col})", "date_trunc('second', {ts_col})"),

    # Date part function
    BenchmarkFunction("date_part_year", "date_part('year', {ts_col})", "date_part('year', {ts_col})"),
    BenchmarkFunction("date_part_month", "date_part('month', {ts_col})", "date_part('month', {ts_col})"),
    BenchmarkFunction("date_part_day", "date_part('day', {ts_col})", "date_part('day', {ts_col})"),
    BenchmarkFunction("date_part_hour", "date_part('hour', {ts_col})", "date_part('hour', {ts_col})"),
    BenchmarkFunction("date_part_dow", "date_part('dow', {ts_col})", "date_part('dow', {ts_col})"),
    BenchmarkFunction("date_part_week", "date_part('week', {ts_col})", "date_part('week', {ts_col})"),

    # Date arithmetic with intervals
    BenchmarkFunction("add_days", "{ts_col} + INTERVAL '7' DAY", "{ts_col} + INTERVAL '7 days'"),
    BenchmarkFunction("sub_days", "{ts_col} - INTERVAL '7' DAY", "{ts_col} - INTERVAL '7 days'"),
    BenchmarkFunction("add_months", "{ts_col} + INTERVAL '1' MONTH", "{ts_col} + INTERVAL '1 month'"),
    BenchmarkFunction("add_hours", "{ts_col} + INTERVAL '12' HOUR", "{ts_col} + INTERVAL '12 hours'"),
    BenchmarkFunction("add_minutes", "{ts_col} + INTERVAL '30' MINUTE", "{ts_col} + INTERVAL '30 minutes'"),

    # Formatting (to_char in DataFusion, strftime in DuckDB)
    BenchmarkFunction("to_char_date", "to_char({ts_col}, '%Y-%m-%d')", "strftime({ts_col}, '%Y-%m-%d')"),
    BenchmarkFunction("to_char_datetime", "to_char({ts_col}, '%Y-%m-%d %H:%M:%S')", "strftime({ts_col}, '%Y-%m-%d %H:%M:%S')"),
    BenchmarkFunction("to_char_time", "to_char({ts_col}, '%H:%M:%S')", "strftime({ts_col}, '%H:%M:%S')"),
]


def generate_temporal_data(num_rows: int) -> pa.Table:
    """Generate test data for temporal functions."""
    random.seed(42)

    base_date = datetime(2020, 1, 1)
    timestamps = []
    timestamps2 = []

    for i in range(num_rows):
        # Generate timestamps spread over 4 years with varying patterns
        days_offset = random.randint(0, 1460)  # ~4 years
        hours_offset = random.randint(0, 23)
        minutes_offset = random.randint(0, 59)
        seconds_offset = random.randint(0, 59)

        ts = base_date + timedelta(
            days=days_offset,
            hours=hours_offset,
            minutes=minutes_offset,
            seconds=seconds_offset
        )
        timestamps.append(ts)

        # Second timestamp for date_diff operations (1-30 days later)
        ts2 = ts + timedelta(days=random.randint(1, 30))
        timestamps2.append(ts2)

    return pa.table({
        'ts_col': pa.array(timestamps, type=pa.timestamp('us')),
        'ts_col2': pa.array(timestamps2, type=pa.timestamp('us')),
    })


# =============================================================================
# CONDITIONAL EXPRESSIONS SUITE
# =============================================================================

CONDITIONAL_FUNCTIONS = [
    # Simple CASE WHEN (2-3 branches)
    BenchmarkFunction(
        "case_2_branches",
        "CASE WHEN {int_col} > 50 THEN 'high' ELSE 'low' END",
        "CASE WHEN {int_col} > 50 THEN 'high' ELSE 'low' END"
    ),
    BenchmarkFunction(
        "case_3_branches",
        "CASE WHEN {int_col} > 66 THEN 'high' WHEN {int_col} > 33 THEN 'medium' ELSE 'low' END",
        "CASE WHEN {int_col} > 66 THEN 'high' WHEN {int_col} > 33 THEN 'medium' ELSE 'low' END"
    ),

    # Medium CASE WHEN (5 branches)
    BenchmarkFunction(
        "case_5_branches",
        "CASE WHEN {int_col} > 80 THEN 'A' WHEN {int_col} > 60 THEN 'B' WHEN {int_col} > 40 THEN 'C' WHEN {int_col} > 20 THEN 'D' ELSE 'F' END",
        "CASE WHEN {int_col} > 80 THEN 'A' WHEN {int_col} > 60 THEN 'B' WHEN {int_col} > 40 THEN 'C' WHEN {int_col} > 20 THEN 'D' ELSE 'F' END"
    ),

    # Complex CASE WHEN (10 branches)
    BenchmarkFunction(
        "case_10_branches",
        "CASE WHEN {int_col} >= 90 THEN 'A+' WHEN {int_col} >= 85 THEN 'A' WHEN {int_col} >= 80 THEN 'A-' WHEN {int_col} >= 75 THEN 'B+' WHEN {int_col} >= 70 THEN 'B' WHEN {int_col} >= 65 THEN 'B-' WHEN {int_col} >= 60 THEN 'C+' WHEN {int_col} >= 55 THEN 'C' WHEN {int_col} >= 50 THEN 'C-' ELSE 'F' END",
        "CASE WHEN {int_col} >= 90 THEN 'A+' WHEN {int_col} >= 85 THEN 'A' WHEN {int_col} >= 80 THEN 'A-' WHEN {int_col} >= 75 THEN 'B+' WHEN {int_col} >= 70 THEN 'B' WHEN {int_col} >= 65 THEN 'B-' WHEN {int_col} >= 60 THEN 'C+' WHEN {int_col} >= 55 THEN 'C' WHEN {int_col} >= 50 THEN 'C-' ELSE 'F' END"
    ),

    # Simple CASE (value matching)
    BenchmarkFunction(
        "case_simple_match",
        "CASE {category} WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 ELSE 0 END",
        "CASE {category} WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 ELSE 0 END"
    ),
    BenchmarkFunction(
        "case_simple_match_10",
        "CASE {category} WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 WHEN 'E' THEN 5 WHEN 'F' THEN 6 WHEN 'G' THEN 7 WHEN 'H' THEN 8 WHEN 'I' THEN 9 WHEN 'J' THEN 10 ELSE 0 END",
        "CASE {category} WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 WHEN 'E' THEN 5 WHEN 'F' THEN 6 WHEN 'G' THEN 7 WHEN 'H' THEN 8 WHEN 'I' THEN 9 WHEN 'J' THEN 10 ELSE 0 END"
    ),

    # CASE with multiple conditions per branch
    BenchmarkFunction(
        "case_multi_condition",
        "CASE WHEN {int_col} > 50 AND {float_col} > 0.5 THEN 'both_high' WHEN {int_col} > 50 OR {float_col} > 0.5 THEN 'one_high' ELSE 'both_low' END",
        "CASE WHEN {int_col} > 50 AND {float_col} > 0.5 THEN 'both_high' WHEN {int_col} > 50 OR {float_col} > 0.5 THEN 'one_high' ELSE 'both_low' END"
    ),

    # Nested CASE expressions
    BenchmarkFunction(
        "case_nested_2_levels",
        "CASE WHEN {int_col} > 50 THEN CASE WHEN {float_col} > 0.5 THEN 'high_high' ELSE 'high_low' END ELSE CASE WHEN {float_col} > 0.5 THEN 'low_high' ELSE 'low_low' END END",
        "CASE WHEN {int_col} > 50 THEN CASE WHEN {float_col} > 0.5 THEN 'high_high' ELSE 'high_low' END ELSE CASE WHEN {float_col} > 0.5 THEN 'low_high' ELSE 'low_low' END END"
    ),
    BenchmarkFunction(
        "case_nested_3_levels",
        "CASE WHEN {int_col} > 66 THEN CASE WHEN {float_col} > 0.66 THEN CASE WHEN {int_col2} > 66 THEN 'HHH' ELSE 'HHL' END ELSE 'HL' END WHEN {int_col} > 33 THEN 'M' ELSE 'L' END",
        "CASE WHEN {int_col} > 66 THEN CASE WHEN {float_col} > 0.66 THEN CASE WHEN {int_col2} > 66 THEN 'HHH' ELSE 'HHL' END ELSE 'HL' END WHEN {int_col} > 33 THEN 'M' ELSE 'L' END"
    ),

    # CASE with expressions in THEN clauses
    BenchmarkFunction(
        "case_expr_result",
        "CASE WHEN {int_col} > 50 THEN {int_col} * 2 + {float_col} ELSE {int_col} / 2 - {float_col} END",
        "CASE WHEN {int_col} > 50 THEN {int_col} * 2 + {float_col} ELSE {int_col} / 2 - {float_col} END"
    ),
    BenchmarkFunction(
        "case_string_concat",
        "CASE WHEN {int_col} > 50 THEN concat({str_col}, '_high') ELSE concat({str_col}, '_low') END",
        "CASE WHEN {int_col} > 50 THEN concat({str_col}, '_high') ELSE concat({str_col}, '_low') END"
    ),

    # COALESCE expressions
    BenchmarkFunction(
        "coalesce_2",
        "COALESCE({nullable_col}, 0)",
        "COALESCE({nullable_col}, 0)"
    ),
    BenchmarkFunction(
        "coalesce_3",
        "COALESCE({nullable_col}, {nullable_col2}, 0)",
        "COALESCE({nullable_col}, {nullable_col2}, 0)"
    ),
    BenchmarkFunction(
        "coalesce_5",
        "COALESCE({nullable_col}, {nullable_col2}, {nullable_col}, {nullable_col2}, 0)",
        "COALESCE({nullable_col}, {nullable_col2}, {nullable_col}, {nullable_col2}, 0)"
    ),

    # NULLIF expressions
    BenchmarkFunction(
        "nullif_int",
        "NULLIF({int_col}, 50)",
        "NULLIF({int_col}, 50)"
    ),
    BenchmarkFunction(
        "nullif_string",
        "NULLIF({category}, 'A')",
        "NULLIF({category}, 'A')"
    ),

    # CASE with NULL handling
    BenchmarkFunction(
        "case_null_check",
        "CASE WHEN {nullable_col} IS NULL THEN 'missing' WHEN {nullable_col} > 50 THEN 'high' ELSE 'low' END",
        "CASE WHEN {nullable_col} IS NULL THEN 'missing' WHEN {nullable_col} > 50 THEN 'high' ELSE 'low' END"
    ),
    BenchmarkFunction(
        "case_null_propagation",
        "CASE WHEN {int_col} > 50 THEN {nullable_col} ELSE {nullable_col2} END",
        "CASE WHEN {int_col} > 50 THEN {nullable_col} ELSE {nullable_col2} END"
    ),

    # Complex real-world scenarios
    BenchmarkFunction(
        "case_bucketing",
        "CASE WHEN {float_col} < 0.1 THEN 0 WHEN {float_col} < 0.2 THEN 1 WHEN {float_col} < 0.3 THEN 2 WHEN {float_col} < 0.4 THEN 3 WHEN {float_col} < 0.5 THEN 4 WHEN {float_col} < 0.6 THEN 5 WHEN {float_col} < 0.7 THEN 6 WHEN {float_col} < 0.8 THEN 7 WHEN {float_col} < 0.9 THEN 8 ELSE 9 END",
        "CASE WHEN {float_col} < 0.1 THEN 0 WHEN {float_col} < 0.2 THEN 1 WHEN {float_col} < 0.3 THEN 2 WHEN {float_col} < 0.4 THEN 3 WHEN {float_col} < 0.5 THEN 4 WHEN {float_col} < 0.6 THEN 5 WHEN {float_col} < 0.7 THEN 6 WHEN {float_col} < 0.8 THEN 7 WHEN {float_col} < 0.9 THEN 8 ELSE 9 END"
    ),
    BenchmarkFunction(
        "case_range_lookup",
        "CASE WHEN {int_col} BETWEEN 0 AND 10 THEN 'tier1' WHEN {int_col} BETWEEN 11 AND 25 THEN 'tier2' WHEN {int_col} BETWEEN 26 AND 50 THEN 'tier3' WHEN {int_col} BETWEEN 51 AND 75 THEN 'tier4' ELSE 'tier5' END",
        "CASE WHEN {int_col} BETWEEN 0 AND 10 THEN 'tier1' WHEN {int_col} BETWEEN 11 AND 25 THEN 'tier2' WHEN {int_col} BETWEEN 26 AND 50 THEN 'tier3' WHEN {int_col} BETWEEN 51 AND 75 THEN 'tier4' ELSE 'tier5' END"
    ),
    BenchmarkFunction(
        "case_complex_business",
        "CASE WHEN {category} = 'A' AND {int_col} > 80 THEN 'premium_a' WHEN {category} = 'A' THEN 'standard_a' WHEN {category} = 'B' AND {float_col} > 0.7 THEN 'premium_b' WHEN {category} = 'B' THEN 'standard_b' WHEN {category} IN ('C', 'D', 'E') THEN 'bulk' ELSE 'other' END",
        "CASE WHEN {category} = 'A' AND {int_col} > 80 THEN 'premium_a' WHEN {category} = 'A' THEN 'standard_a' WHEN {category} = 'B' AND {float_col} > 0.7 THEN 'premium_b' WHEN {category} = 'B' THEN 'standard_b' WHEN {category} IN ('C', 'D', 'E') THEN 'bulk' ELSE 'other' END"
    ),

    # Boolean result CASE
    BenchmarkFunction(
        "case_boolean_result",
        "CASE WHEN {int_col} > 50 THEN TRUE ELSE FALSE END",
        "CASE WHEN {int_col} > 50 THEN TRUE ELSE FALSE END"
    ),

    # GREATEST / LEAST (related conditional functions)
    BenchmarkFunction(
        "greatest_2",
        "GREATEST({int_col}, {int_col2})",
        "GREATEST({int_col}, {int_col2})"
    ),
    BenchmarkFunction(
        "least_2",
        "LEAST({int_col}, {int_col2})",
        "LEAST({int_col}, {int_col2})"
    ),
    BenchmarkFunction(
        "greatest_3",
        "GREATEST({int_col}, {int_col2}, {nullable_col})",
        "GREATEST({int_col}, {int_col2}, {nullable_col})"
    ),
]


def generate_conditional_data(num_rows: int) -> pa.Table:
    """Generate test data for conditional expressions."""
    random.seed(42)

    int_col = [random.randint(0, 100) for _ in range(num_rows)]
    int_col2 = [random.randint(0, 100) for _ in range(num_rows)]
    float_col = [random.random() for _ in range(num_rows)]

    # Nullable columns (30% nulls)
    nullable_col = [random.randint(0, 100) if random.random() > 0.3 else None for _ in range(num_rows)]
    nullable_col2 = [random.randint(0, 100) if random.random() > 0.3 else None for _ in range(num_rows)]

    # Category column (A-J)
    categories = [chr(ord('A') + random.randint(0, 9)) for _ in range(num_rows)]

    # String column
    str_col = [f"item_{i % 1000}" for i in range(num_rows)]

    return pa.table({
        'int_col': pa.array(int_col, type=pa.int64()),
        'int_col2': pa.array(int_col2, type=pa.int64()),
        'float_col': pa.array(float_col, type=pa.float64()),
        'nullable_col': pa.array(nullable_col, type=pa.int64()),
        'nullable_col2': pa.array(nullable_col2, type=pa.int64()),
        'category': pa.array(categories, type=pa.string()),
        'str_col': pa.array(str_col, type=pa.string()),
    })


# =============================================================================
# BENCHMARK SUITES REGISTRY
# =============================================================================

SUITES = {
    'string': {
        'name': 'String Functions',
        'functions': STRING_FUNCTIONS,
        'data_generator': generate_string_data,
        'columns': {'str_col': 'str_col'},
    },
    'temporal': {
        'name': 'Temporal Functions',
        'functions': TEMPORAL_FUNCTIONS,
        'data_generator': generate_temporal_data,
        'columns': {'ts_col': 'ts_col'},
    },
    'conditional': {
        'name': 'Conditional Expressions',
        'functions': CONDITIONAL_FUNCTIONS,
        'data_generator': generate_conditional_data,
        'columns': {
            'int_col': 'int_col',
            'int_col2': 'int_col2',
            'float_col': 'float_col',
            'nullable_col': 'nullable_col',
            'nullable_col2': 'nullable_col2',
            'category': 'category',
            'str_col': 'str_col',
        },
    },
}


# =============================================================================
# BENCHMARK INFRASTRUCTURE
# =============================================================================

def setup_datafusion(parquet_path: str) -> datafusion.SessionContext:
    """Create and configure DataFusion context."""
    ctx = datafusion.SessionContext()
    ctx.register_parquet('test_data', parquet_path)
    return ctx


def setup_duckdb(parquet_path: str) -> duckdb.DuckDBPyConnection:
    """Create and configure DuckDB connection."""
    conn = duckdb.connect(':memory:')
    conn.execute(f"CREATE VIEW test_data AS SELECT * FROM read_parquet('{parquet_path}')")
    return conn


def benchmark_datafusion(ctx: datafusion.SessionContext, expr: str,
                         warmup: int = 2, iterations: int = 5) -> float:
    """Benchmark a query in DataFusion, return average time in ms."""
    query = f"SELECT {expr} FROM test_data"

    for _ in range(warmup):
        ctx.sql(query).collect()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        ctx.sql(query).collect()
        end = time.perf_counter()
        times.append((end - start) * 1000)

    return sum(times) / len(times)


def benchmark_duckdb(conn: duckdb.DuckDBPyConnection, expr: str,
                     warmup: int = 2, iterations: int = 5) -> float:
    """Benchmark a query in DuckDB, return average time in ms."""
    query = f"SELECT {expr} FROM test_data"

    for _ in range(warmup):
        conn.execute(query).fetchall()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        conn.execute(query).fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)

    return sum(times) / len(times)


def run_benchmarks(suite_name: str,
                   num_rows: int = 1_000_000,
                   warmup: int = 2,
                   iterations: int = 5) -> list[BenchmarkResult]:
    """Run benchmarks for a specific suite and return results."""
    suite = SUITES[suite_name]
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, 'test_data.parquet')

        print(f"Generating {num_rows:,} rows of test data...")
        table = suite['data_generator'](num_rows)
        pq.write_table(table, parquet_path)
        print(f"Parquet file written to: {parquet_path}")
        print(f"File size: {os.path.getsize(parquet_path) / 1024 / 1024:.2f} MB")

        print("\nSetting up DataFusion...")
        df_ctx = setup_datafusion(parquet_path)

        print("Setting up DuckDB...")
        duck_conn = setup_duckdb(parquet_path)

        print(f"\nRunning benchmarks ({warmup} warmup, {iterations} iterations each)...\n")

        columns = suite['columns']
        for func in suite['functions']:
            df_expr = func.datafusion_expr.format(**columns)
            duck_expr = func.duckdb_expr.format(**columns)

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

            if df_time == df_time and duck_time == duck_time:
                faster = "DataFusion" if df_time < duck_time else "DuckDB"
                ratio = max(df_time, duck_time) / min(df_time, duck_time)
                print(f"done ({faster} {ratio:.2f}x faster)")
            else:
                print("done (with errors)")

        duck_conn.close()

    return results


def format_results_markdown(results: list[BenchmarkResult], suite_name: str) -> str:
    """Format benchmark results as a markdown table."""
    suite = SUITES[suite_name]
    lines = [
        f"# {suite['name']} Microbenchmarks: DataFusion vs DuckDB",
        "",
        f"**Rows:** {results[0].rows:,}",
        "",
        "| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |",
        "|----------|----------------:|------------:|--------:|--------|",
    ]

    for r in results:
        if r.datafusion_time_ms != r.datafusion_time_ms or r.duckdb_time_ms != r.duckdb_time_ms:
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
        description="Benchmark SQL functions: DataFusion vs DuckDB"
    )
    parser.add_argument(
        "--suite", type=str, default="string",
        choices=list(SUITES.keys()),
        help=f"Benchmark suite to run: {', '.join(SUITES.keys())} (default: string)"
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

    args = parser.parse_args()

    suite = SUITES[args.suite]
    print("=" * 60)
    print(f"{suite['name']} Microbenchmarks: DataFusion vs DuckDB")
    print("=" * 60)

    results = run_benchmarks(
        suite_name=args.suite,
        num_rows=args.rows,
        warmup=args.warmup,
        iterations=args.iterations
    )

    markdown = format_results_markdown(results, args.suite)

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
