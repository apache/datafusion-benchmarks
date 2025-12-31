"""String functions benchmark suite."""

import random
import string

import pyarrow as pa

from . import BenchmarkFunction, Suite


FUNCTIONS = [
    BenchmarkFunction("trim", "trim({col})", "trim({col})"),
    BenchmarkFunction("ltrim", "ltrim({col})", "ltrim({col})"),
    BenchmarkFunction("rtrim", "rtrim({col})", "rtrim({col})"),
    BenchmarkFunction("lower", "lower({col})", "lower({col})"),
    BenchmarkFunction("upper", "upper({col})", "upper({col})"),
    BenchmarkFunction("length", "length({col})", "length({col})"),
    BenchmarkFunction("char_length", "char_length({col})", "length({col})"),
    BenchmarkFunction("reverse", "reverse({col})", "reverse({col})"),
    BenchmarkFunction("repeat_3", "repeat({col}, 3)", "repeat({col}, 3)"),
    BenchmarkFunction("concat", "concat({col}, {col})", "concat({col}, {col})"),
    BenchmarkFunction("concat_ws", "concat_ws('-', {col}, {col})", "concat_ws('-', {col}, {col})"),
    BenchmarkFunction("substring_1_5", "substring({col}, 1, 5)", "substring({col}, 1, 5)"),
    BenchmarkFunction("left_5", "left({col}, 5)", "left({col}, 5)"),
    BenchmarkFunction("right_5", "right({col}, 5)", "right({col}, 5)"),
    BenchmarkFunction("lpad_20", "lpad({col}, 20, '*')", "lpad({col}, 20, '*')"),
    BenchmarkFunction("rpad_20", "rpad({col}, 20, '*')", "rpad({col}, 20, '*')"),
    BenchmarkFunction("replace", "replace({col}, 'a', 'X')", "replace({col}, 'a', 'X')"),
    BenchmarkFunction("translate", "translate({col}, 'aeiou', '12345')", "translate({col}, 'aeiou', '12345')"),
    BenchmarkFunction("ascii", "ascii({col})", "ascii({col})"),
    BenchmarkFunction("md5", "md5({col})", "md5({col})"),
    BenchmarkFunction("sha256", "sha256({col})", "sha256({col})"),
    BenchmarkFunction("btrim", "btrim({col}, ' ')", "trim({col}, ' ')"),
    BenchmarkFunction("split_part", "split_part({col}, ' ', 1)", "split_part({col}, ' ', 1)"),
    BenchmarkFunction("starts_with", "starts_with({col}, 'test')", "starts_with({col}, 'test')"),
    BenchmarkFunction("ends_with", "ends_with({col}, 'data')", "ends_with({col}, 'data')"),
    BenchmarkFunction("strpos", "strpos({col}, 'e')", "strpos({col}, 'e')"),
    BenchmarkFunction("regexp_replace", "regexp_replace({col}, '[aeiou]', '*')", "regexp_replace({col}, '[aeiou]', '*', 'g')"),
]


def generate_data(num_rows: int = 1_000_000, use_string_view: bool = False) -> pa.Table:
    """Generate test data with various string patterns."""
    random.seed(42)  # For reproducibility

    strings_data = []
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
        strings_data.append(s)

    str_type = pa.string_view() if use_string_view else pa.string()
    return pa.table({
        'str_col': pa.array(strings_data, type=str_type)
    })


SUITE = Suite(
    name="strings",
    description="String function benchmarks",
    column_name="str_col",
    functions=FUNCTIONS,
    generate_data=generate_data,
)
