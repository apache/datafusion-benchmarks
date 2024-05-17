import argparse
from pyspark.sql import SparkSession
import time

def main(benchmark: str, data_path: str, query_path: str):

    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("DataFusion Benchmark derived from TPC-H / TPC-DS") \
        .getOrCreate()

    # Register the tables
    if benchmark == "tpch":
        table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    elif benchmark == "tpcds":
        raise "tpcds not implemented yet"
    else:
        raise "invalid benchmark"

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)

    for query in range(1, 23):
        # read text file
        path = f"{query_path}/q{query}.sql"
        print(f"Reading query {query} using path {path}")
        with open(path, "r") as f:
            text = f.read()
            # each file can contain multiple queries
            queries = text.split(";")

            start_time = time.time()
            for sql in queries:
                sql = sql.strip()
                if len(sql) > 0:
                    print(f"Executing: {sql}")
                    df = spark.sql(sql)
                    rows = df.collect()

                    print(f"Query {query} returned {len(rows)} rows")
            end_time = time.time()
            print(f"Query {query} took {end_time - start_time} seconds")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    args = parser.parse_args()

    main(args.benchmark, args.data, args.queries)