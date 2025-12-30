# String Functions Microbenchmarks: DataFusion vs DuckDB

**Rows:** 1,000,000

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| trim | 46.57 | 123.44 | 2.65x | DataFusion |
| ltrim | 45.19 | 58.57 | 1.30x | DataFusion |
| rtrim | 43.32 | 114.99 | 2.65x | DataFusion |
| lower | 44.93 | 63.10 | 1.40x | DataFusion |
| upper | 39.99 | 67.38 | 1.68x | DataFusion |
| length | 22.11 | 26.54 | 1.20x | DataFusion |
| char_length | 23.50 | 26.75 | 1.14x | DataFusion |
| reverse | 36.64 | 60.70 | 1.66x | DataFusion |
| repeat_3 | 46.49 | 75.45 | 1.62x | DataFusion |
| concat | 70.13 | 67.12 | 1.04x | DuckDB |
| concat_ws | 36.96 | 73.64 | 1.99x | DataFusion |
| substring_1_5 | 35.43 | 41.34 | 1.17x | DataFusion |
| left_5 | 37.96 | 47.62 | 1.25x | DataFusion |
| right_5 | 63.57 | 60.91 | 1.04x | DuckDB |
| lpad_20 | 341.24 | 94.19 | 3.62x | DuckDB |
| rpad_20 | 343.67 | 94.86 | 3.62x | DuckDB |
| replace | 51.88 | 106.83 | 2.06x | DataFusion |
| translate | 768.85 | 299.88 | 2.56x | DuckDB |
| ascii | 19.25 | 23.28 | 1.21x | DataFusion |
| md5 | 283.36 | 139.64 | 2.03x | DuckDB |
| sha256 | 61.61 | 265.46 | 4.31x | DataFusion |
| btrim | 38.98 | 128.57 | 3.30x | DataFusion |
| split_part | 77.90 | 57.19 | 1.36x | DuckDB |
| starts_with | 19.47 | 26.80 | 1.38x | DataFusion |
| ends_with | 27.12 | 22.07 | 1.23x | DuckDB |
| strpos | 43.98 | 29.50 | 1.49x | DuckDB |
| regexp_replace | 93.95 | 410.35 | 4.37x | DataFusion |

## Summary

- **Functions tested:** 27
- **DataFusion faster:** 18 functions
- **DuckDB faster:** 9 functions
- **Total DataFusion time:** 2764.06 ms
- **Total DuckDB time:** 2606.18 ms