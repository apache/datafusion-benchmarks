"""Temporal (date/time) functions benchmark suite."""

import random
from datetime import datetime, timedelta

import pyarrow as pa

from . import BenchmarkFunction, Suite


FUNCTIONS = [
    # Date extraction functions
    BenchmarkFunction("year", "year({col})", "year({col})"),
    BenchmarkFunction("month", "month({col})", "month({col})"),
    BenchmarkFunction("day", "day({col})", "day({col})"),
    BenchmarkFunction("hour", "hour({col})", "hour({col})"),
    BenchmarkFunction("minute", "minute({col})", "minute({col})"),
    BenchmarkFunction("second", "second({col})", "second({col})"),
    BenchmarkFunction("week", "week({col})", "week({col})"),
    BenchmarkFunction("quarter", "quarter({col})", "quarter({col})"),
    BenchmarkFunction("day_of_week", "extract(dow from {col})", "dayofweek({col})"),
    BenchmarkFunction("day_of_year", "extract(doy from {col})", "dayofyear({col})"),

    # Date truncation
    BenchmarkFunction("date_trunc_day", "date_trunc('day', {col})", "date_trunc('day', {col})"),
    BenchmarkFunction("date_trunc_month", "date_trunc('month', {col})", "date_trunc('month', {col})"),
    BenchmarkFunction("date_trunc_year", "date_trunc('year', {col})", "date_trunc('year', {col})"),
    BenchmarkFunction("date_trunc_hour", "date_trunc('hour', {col})", "date_trunc('hour', {col})"),

    # Date arithmetic
    BenchmarkFunction("date_add_days", "{col} + interval '7 days'", "{col} + interval '7 days'"),
    BenchmarkFunction("date_sub_days", "{col} - interval '7 days'", "{col} - interval '7 days'"),
    BenchmarkFunction("date_add_months", "{col} + interval '1 month'", "{col} + interval '1 month'"),

    # Date formatting/parsing
    BenchmarkFunction("to_char", "to_char({col}, '%Y-%m-%d')", "strftime({col}, '%Y-%m-%d')"),

    # Date parts
    BenchmarkFunction("date_part_hour", "date_part('hour', {col})", "date_part('hour', {col})"),
    BenchmarkFunction("date_part_minute", "date_part('minute', {col})", "date_part('minute', {col})"),

    # Current date/time comparisons
    BenchmarkFunction("is_past", "{col} < now()", "{col} < now()"),
]


def generate_data(num_rows: int = 1_000_000, use_string_view: bool = False) -> pa.Table:
    """Generate test data with various timestamp patterns."""
    random.seed(42)  # For reproducibility

    # Generate timestamps spanning several years
    base_date = datetime(2020, 1, 1)
    max_days = 365 * 5  # 5 years of data

    timestamps = []
    for i in range(num_rows):
        # Mix of different timestamp patterns
        pattern_type = i % 4
        if pattern_type == 0:
            # Random timestamp within range
            days = random.randint(0, max_days)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)
            ts = base_date + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        elif pattern_type == 1:
            # Timestamps at midnight (common pattern)
            days = random.randint(0, max_days)
            ts = base_date + timedelta(days=days)
        elif pattern_type == 2:
            # Timestamps at specific hours (business hours)
            days = random.randint(0, max_days)
            hours = random.choice([9, 10, 11, 12, 13, 14, 15, 16, 17])
            ts = base_date + timedelta(days=days, hours=hours)
        else:
            # Sequential timestamps (time series pattern)
            ts = base_date + timedelta(seconds=i)
        timestamps.append(ts)

    return pa.table({
        'ts_col': pa.array(timestamps, type=pa.timestamp('us'))
    })


SUITE = Suite(
    name="temporal",
    description="Date/time function benchmarks",
    column_name="ts_col",
    functions=FUNCTIONS,
    generate_data=generate_data,
)
