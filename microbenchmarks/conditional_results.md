# Conditional Expressions Microbenchmarks: DataFusion vs DuckDB

**Rows:** 1,000,000

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| case_2_branches | 17.67 | 16.57 | 1.07x | DuckDB |
| case_3_branches | 42.93 | 21.81 | 1.97x | DuckDB |
| case_5_branches | 59.96 | 18.96 | 3.16x | DuckDB |
| case_10_branches | 87.85 | 25.07 | 3.50x | DuckDB |
| case_simple_match | 41.32 | 7.72 | 5.35x | DuckDB |
| case_simple_match_10 | 100.42 | 16.69 | 6.02x | DuckDB |
| case_multi_condition | 51.24 | 32.91 | 1.56x | DuckDB |
| case_nested_2_levels | 55.03 | 31.26 | 1.76x | DuckDB |
| case_nested_3_levels | 66.14 | 24.20 | 2.73x | DuckDB |
| case_expr_result | 33.84 | 15.87 | 2.13x | DuckDB |
| case_string_concat | 74.08 | 24.31 | 3.05x | DuckDB |
| coalesce_2 | 17.79 | 7.55 | 2.35x | DuckDB |
| coalesce_3 | 20.66 | 10.73 | 1.93x | DuckDB |
| coalesce_5 | 22.41 | 11.48 | 1.95x | DuckDB |
| nullif_int | 4.99 | 6.30 | 1.26x | DataFusion |
| nullif_string | 9.62 | 12.77 | 1.33x | DataFusion |
| case_null_check | 42.70 | 17.38 | 2.46x | DuckDB |
| case_null_propagation | 29.92 | 16.20 | 1.85x | DuckDB |
| case_bucketing | 77.56 | 32.37 | 2.40x | DuckDB |
| case_range_lookup | 55.64 | 19.65 | 2.83x | DuckDB |
| case_complex_business | 84.08 | 32.88 | 2.56x | DuckDB |
| case_boolean_result | 3.98 | 9.74 | 2.45x | DataFusion |
| greatest_2 | 8.56 | 7.99 | 1.07x | DuckDB |
| least_2 | 8.46 | 7.62 | 1.11x | DuckDB |
| greatest_3 | 19.38 | 13.85 | 1.40x | DuckDB |

## Summary

- **Functions tested:** 25
- **DataFusion faster:** 3 functions
- **DuckDB faster:** 22 functions
- **Total DataFusion time:** 1036.25 ms
- **Total DuckDB time:** 441.88 ms