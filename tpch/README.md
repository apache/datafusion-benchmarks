# TPC-H

## Generating TPC-H data with Spark

Databricks provides tooling for generating TPC-H datasets in a Spark cluster:

[https://github.com/databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf)

## Generating TPC-H data without Spark

For local development and testing, we provide a Python script to generate TPC-H CSV data and convert it into Parquet, 
using DataFusion.