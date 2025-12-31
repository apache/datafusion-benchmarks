"""Numeric/math functions benchmark suite."""

import random

import pyarrow as pa

from . import BenchmarkFunction, Suite


FUNCTIONS = [
    # Basic math
    BenchmarkFunction("abs", "abs({col})", "abs({col})"),
    BenchmarkFunction("ceil", "ceil({col})", "ceil({col})"),
    BenchmarkFunction("floor", "floor({col})", "floor({col})"),
    BenchmarkFunction("round", "round({col}, 2)", "round({col}, 2)"),
    BenchmarkFunction("trunc", "trunc({col})", "trunc({col})"),
    BenchmarkFunction("signum", "signum({col})", "sign({col})"),

    # Powers and roots
    BenchmarkFunction("sqrt", "sqrt(abs({col}))", "sqrt(abs({col}))"),
    BenchmarkFunction("cbrt", "cbrt({col})", "cbrt({col})"),
    BenchmarkFunction("power", "power({col}, 2)", "power({col}, 2)"),
    BenchmarkFunction("exp", "exp({col} / 100)", "exp({col} / 100)"),

    # Logarithms
    BenchmarkFunction("ln", "ln(abs({col}) + 1)", "ln(abs({col}) + 1)"),
    BenchmarkFunction("log10", "log10(abs({col}) + 1)", "log10(abs({col}) + 1)"),
    BenchmarkFunction("log2", "log2(abs({col}) + 1)", "log2(abs({col}) + 1)"),
    BenchmarkFunction("log", "log(2, abs({col}) + 1)", "log(2, abs({col}) + 1)"),

    # Trigonometric
    BenchmarkFunction("sin", "sin({col})", "sin({col})"),
    BenchmarkFunction("cos", "cos({col})", "cos({col})"),
    BenchmarkFunction("tan", "tan({col})", "tan({col})"),
    BenchmarkFunction("asin", "asin(sin({col}))", "asin(sin({col}))"),
    BenchmarkFunction("acos", "acos(cos({col}))", "acos(cos({col}))"),
    BenchmarkFunction("atan", "atan({col})", "atan({col})"),
    BenchmarkFunction("atan2", "atan2({col}, {col} + 1)", "atan2({col}, {col} + 1)"),

    # Hyperbolic
    BenchmarkFunction("sinh", "sinh({col} / 100)", "sinh({col} / 100)"),
    BenchmarkFunction("cosh", "cosh({col} / 100)", "cosh({col} / 100)"),
    BenchmarkFunction("tanh", "tanh({col})", "tanh({col})"),

    # Other math functions
    BenchmarkFunction("degrees", "degrees({col})", "degrees({col})"),
    BenchmarkFunction("radians", "radians({col})", "radians({col})"),
    BenchmarkFunction("pi", "pi() * {col}", "pi() * {col}"),
    BenchmarkFunction("mod", "CAST({col} AS BIGINT) % 7", "CAST({col} AS BIGINT) % 7"),
    BenchmarkFunction("gcd", "gcd(CAST({col} AS BIGINT), 12)", "gcd(CAST({col} AS BIGINT), 12)"),
    BenchmarkFunction("lcm", "lcm(CAST(abs({col}) AS BIGINT) % 1000 + 1, 12)", "lcm(CAST(abs({col}) AS BIGINT) % 1000 + 1, 12)"),
    BenchmarkFunction("factorial", "factorial(CAST(abs({col}) AS BIGINT) % 20)", "factorial(CAST(abs({col}) AS INTEGER) % 20)"),

    # Comparison
    BenchmarkFunction("greatest", "greatest({col}, {col} * 2, 0)", "greatest({col}, {col} * 2, 0)"),
    BenchmarkFunction("least", "least({col}, {col} * 2, 0)", "least({col}, {col} * 2, 0)"),

    # Null handling with numeric
    BenchmarkFunction("coalesce", "coalesce({col}, 0)", "coalesce({col}, 0)"),
    BenchmarkFunction("nullif", "nullif({col}, 0)", "nullif({col}, 0)"),

    # Bitwise (on integers)
    BenchmarkFunction("bit_and", "CAST({col} AS BIGINT) & 255", "CAST({col} AS BIGINT) & 255"),
    BenchmarkFunction("bit_or", "CAST({col} AS BIGINT) | 255", "CAST({col} AS BIGINT) | 255"),
    BenchmarkFunction("bit_xor", "CAST({col} AS BIGINT) ^ 255", "xor(CAST({col} AS BIGINT), 255)"),
]


def generate_data(num_rows: int = 1_000_000, use_string_view: bool = False) -> pa.Table:
    """Generate test data with various numeric patterns."""
    random.seed(42)  # For reproducibility

    values = []
    for i in range(num_rows):
        pattern_type = i % 5
        if pattern_type == 0:
            # Small integers
            v = random.randint(-100, 100)
        elif pattern_type == 1:
            # Larger integers
            v = random.randint(-10000, 10000)
        elif pattern_type == 2:
            # Floating point values
            v = random.uniform(-1000, 1000)
        elif pattern_type == 3:
            # Small decimals
            v = random.uniform(-1, 1)
        else:
            # Mixed range
            v = random.gauss(0, 500)
        values.append(v)

    return pa.table({
        'num_col': pa.array(values, type=pa.float64())
    })


SUITE = Suite(
    name="numeric",
    description="Numeric function benchmarks",
    column_name="num_col",
    functions=FUNCTIONS,
    generate_data=generate_data,
)
