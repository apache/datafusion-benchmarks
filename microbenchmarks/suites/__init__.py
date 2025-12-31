"""Benchmark suites for microbenchmarks."""

from dataclasses import dataclass
from typing import Callable
import pyarrow as pa


@dataclass
class BenchmarkFunction:
    """Defines a function with syntax for both engines."""
    name: str
    datafusion_expr: str  # Expression using {col} as placeholder for column name
    duckdb_expr: str      # Expression using {col} as placeholder for column name


@dataclass
class Suite:
    """Defines a benchmark suite."""
    name: str
    description: str
    column_name: str
    functions: list[BenchmarkFunction]
    generate_data: Callable[[int, bool], pa.Table]  # (num_rows, use_string_view) -> Table


# Import suites to register them
from . import strings
from . import temporal
from . import numeric

# Registry of available suites
SUITES: dict[str, Suite] = {
    'strings': strings.SUITE,
    'temporal': temporal.SUITE,
    'numeric': numeric.SUITE,
}


def get_suite(name: str) -> Suite:
    """Get a suite by name."""
    if name not in SUITES:
        available = ', '.join(SUITES.keys())
        raise ValueError(f"Unknown suite: {name}. Available: {available}")
    return SUITES[name]


def list_suites() -> list[str]:
    """List available suite names."""
    return list(SUITES.keys())
