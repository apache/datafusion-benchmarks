"""Conditional/logic functions benchmark suite."""

import random

import pyarrow as pa

from . import BenchmarkFunction, Suite


FUNCTIONS = [
    # CASE expressions
    BenchmarkFunction("case_simple",
        "CASE {col} WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
        "CASE {col} WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"),
    BenchmarkFunction("case_searched",
        "CASE WHEN {col} < 0 THEN 'negative' WHEN {col} = 0 THEN 'zero' ELSE 'positive' END",
        "CASE WHEN {col} < 0 THEN 'negative' WHEN {col} = 0 THEN 'zero' ELSE 'positive' END"),
    BenchmarkFunction("case_many_branches",
        "CASE WHEN {col} < -50 THEN 'a' WHEN {col} < -25 THEN 'b' WHEN {col} < 0 THEN 'c' WHEN {col} < 25 THEN 'd' WHEN {col} < 50 THEN 'e' ELSE 'f' END",
        "CASE WHEN {col} < -50 THEN 'a' WHEN {col} < -25 THEN 'b' WHEN {col} < 0 THEN 'c' WHEN {col} < 25 THEN 'd' WHEN {col} < 50 THEN 'e' ELSE 'f' END"),
    BenchmarkFunction("case_nested",
        "CASE WHEN {col} > 0 THEN CASE WHEN {col} > 50 THEN 'high' ELSE 'low' END ELSE 'negative' END",
        "CASE WHEN {col} > 0 THEN CASE WHEN {col} > 50 THEN 'high' ELSE 'low' END ELSE 'negative' END"),

    # NULL handling
    BenchmarkFunction("coalesce_2", "COALESCE(nullable_col, 0)", "COALESCE(nullable_col, 0)"),
    BenchmarkFunction("coalesce_3", "COALESCE(nullable_col, {col}, 0)", "COALESCE(nullable_col, {col}, 0)"),
    BenchmarkFunction("coalesce_many", "COALESCE(nullable_col, NULL, NULL, {col}, 0)", "COALESCE(nullable_col, NULL, NULL, {col}, 0)"),
    BenchmarkFunction("nullif", "NULLIF({col}, 0)", "NULLIF({col}, 0)"),
    BenchmarkFunction("nullif_expr", "NULLIF({col} % 10, 5)", "NULLIF({col} % 10, 5)"),
    BenchmarkFunction("ifnull", "IFNULL(nullable_col, -1)", "IFNULL(nullable_col, -1)"),
    BenchmarkFunction("nvl", "NVL(nullable_col, -1)", "IFNULL(nullable_col, -1)"),

    # Comparison functions
    BenchmarkFunction("greatest_2", "GREATEST({col}, {col} * -1)", "GREATEST({col}, {col} * -1)"),
    BenchmarkFunction("greatest_3", "GREATEST({col}, 0, -100)", "GREATEST({col}, 0, -100)"),
    BenchmarkFunction("least_2", "LEAST({col}, {col} * -1)", "LEAST({col}, {col} * -1)"),
    BenchmarkFunction("least_3", "LEAST({col}, 0, 100)", "LEAST({col}, 0, 100)"),

    # Boolean logic
    BenchmarkFunction("and_simple", "{col} > 0 AND {col} < 50", "{col} > 0 AND {col} < 50"),
    BenchmarkFunction("or_simple", "{col} < -50 OR {col} > 50", "{col} < -50 OR {col} > 50"),
    BenchmarkFunction("not", "NOT ({col} > 0)", "NOT ({col} > 0)"),
    BenchmarkFunction("and_or_mixed", "({col} > 0 AND {col} < 50) OR {col} < -50", "({col} > 0 AND {col} < 50) OR {col} < -50"),
    BenchmarkFunction("complex_bool", "({col} > 0 AND {col} < 25) OR ({col} < 0 AND {col} > -25) OR {col} = 0",
                                      "({col} > 0 AND {col} < 25) OR ({col} < 0 AND {col} > -25) OR {col} = 0"),

    # Comparison operators
    BenchmarkFunction("eq", "{col} = 0", "{col} = 0"),
    BenchmarkFunction("neq", "{col} <> 0", "{col} <> 0"),
    BenchmarkFunction("lt", "{col} < 0", "{col} < 0"),
    BenchmarkFunction("lte", "{col} <= 0", "{col} <= 0"),
    BenchmarkFunction("gt", "{col} > 0", "{col} > 0"),
    BenchmarkFunction("gte", "{col} >= 0", "{col} >= 0"),

    # BETWEEN and IN
    BenchmarkFunction("between", "{col} BETWEEN -50 AND 50", "{col} BETWEEN -50 AND 50"),
    BenchmarkFunction("not_between", "{col} NOT BETWEEN -25 AND 25", "{col} NOT BETWEEN -25 AND 25"),
    BenchmarkFunction("in_list_small", "{col} IN (1, 2, 3, 4, 5)", "{col} IN (1, 2, 3, 4, 5)"),
    BenchmarkFunction("in_list_medium", "{col} IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)", "{col} IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"),
    BenchmarkFunction("not_in", "{col} NOT IN (1, 2, 3, 4, 5)", "{col} NOT IN (1, 2, 3, 4, 5)"),

    # NULL checks
    BenchmarkFunction("is_null", "nullable_col IS NULL", "nullable_col IS NULL"),
    BenchmarkFunction("is_not_null", "nullable_col IS NOT NULL", "nullable_col IS NOT NULL"),

    # IF (conditional expression) - DataFusion uses CASE, DuckDB has IF
    BenchmarkFunction("if_simple",
        "CASE WHEN {col} > 0 THEN 'positive' ELSE 'non-positive' END",
        "IF({col} > 0, 'positive', 'non-positive')"),
    BenchmarkFunction("if_numeric",
        "CASE WHEN {col} > 0 THEN {col} ELSE 0 END",
        "IF({col} > 0, {col}, 0)"),
    BenchmarkFunction("if_nested",
        "CASE WHEN {col} > 0 THEN CASE WHEN {col} > 50 THEN 'high' ELSE 'low' END ELSE 'negative' END",
        "IF({col} > 0, IF({col} > 50, 'high', 'low'), 'negative')"),
]


def generate_data(num_rows: int = 1_000_000, use_string_view: bool = False) -> pa.Table:
    """Generate test data with integers and nullable values."""
    random.seed(42)

    values = []
    nullable_values = []

    for i in range(num_rows):
        # Integer values in range for various conditional tests
        v = random.randint(-100, 100)
        values.append(v)

        # Nullable column: ~30% nulls
        if random.random() < 0.3:
            nullable_values.append(None)
        else:
            nullable_values.append(random.randint(-100, 100))

    return pa.table({
        'val_col': pa.array(values, type=pa.int64()),
        'nullable_col': pa.array(nullable_values, type=pa.int64()),
    })


SUITE = Suite(
    name="conditional",
    description="Conditional/logic function benchmarks",
    column_name="val_col",
    functions=FUNCTIONS,
    generate_data=generate_data,
)
