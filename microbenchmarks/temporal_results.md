# Temporal Functions Microbenchmarks: DataFusion vs DuckDB

**Rows:** 1,000,000

| Function | DataFusion (ms) | DuckDB (ms) | Speedup | Faster |
|----------|----------------:|------------:|--------:|--------|
| extract_year | 18.87 | 10.97 | 1.72x | DuckDB |
| extract_month | 18.80 | 11.27 | 1.67x | DuckDB |
| extract_day | 18.05 | 11.29 | 1.60x | DuckDB |
| extract_hour | 18.40 | 12.94 | 1.42x | DuckDB |
| extract_minute | 18.44 | 14.02 | 1.32x | DuckDB |
| extract_second | 26.27 | 13.17 | 2.00x | DuckDB |
| extract_dow | 19.33 | 13.82 | 1.40x | DuckDB |
| extract_doy | 18.71 | 15.51 | 1.21x | DuckDB |
| extract_week | 20.53 | 29.80 | 1.45x | DataFusion |
| extract_quarter | 17.94 | 18.59 | 1.04x | DataFusion |
| extract_epoch | 11.89 | 11.81 | 1.01x | DuckDB |
| date_trunc_year | 30.51 | 20.60 | 1.48x | DuckDB |
| date_trunc_quarter | 32.21 | 23.21 | 1.39x | DuckDB |
| date_trunc_month | 27.34 | 25.80 | 1.06x | DuckDB |
| date_trunc_week | 29.38 | 12.78 | 2.30x | DuckDB |
| date_trunc_day | 10.58 | 9.93 | 1.07x | DuckDB |
| date_trunc_hour | 10.62 | 22.84 | 2.15x | DataFusion |
| date_trunc_minute | 9.70 | 23.40 | 2.41x | DataFusion |
| date_trunc_second | 10.19 | 21.94 | 2.15x | DataFusion |
| date_part_year | 14.56 | 11.32 | 1.29x | DuckDB |
| date_part_month | 14.82 | 10.99 | 1.35x | DuckDB |
| date_part_day | 15.29 | 11.00 | 1.39x | DuckDB |
| date_part_hour | 15.20 | 13.90 | 1.09x | DuckDB |
| date_part_dow | 16.21 | 13.85 | 1.17x | DuckDB |
| date_part_week | 17.74 | 27.30 | 1.54x | DataFusion |
| add_days | 53.87 | 22.94 | 2.35x | DuckDB |
| sub_days | 56.84 | 23.79 | 2.39x | DuckDB |
| add_months | 59.00 | 38.79 | 1.52x | DuckDB |
| add_hours | 37.52 | 25.44 | 1.47x | DuckDB |
| add_minutes | 39.40 | 23.77 | 1.66x | DuckDB |
| to_char_date | 150.98 | 52.48 | 2.88x | DuckDB |
| to_char_datetime | 216.22 | 95.88 | 2.26x | DuckDB |
| to_char_time | 129.67 | 48.38 | 2.68x | DuckDB |

## Summary

- **Functions tested:** 33
- **DataFusion faster:** 6 functions
- **DuckDB faster:** 27 functions
- **Total DataFusion time:** 1205.07 ms
- **Total DuckDB time:** 743.49 ms