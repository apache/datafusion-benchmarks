<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# TPC-H

## Generating TPC-H data with Spark

Databricks provides tooling for generating TPC-H datasets in a Spark cluster:

[https://github.com/databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf)

## Generating TPC-H data without Spark

For local development and testing, we provide a Python script to generate TPC-H CSV data and convert it into Parquet, 
using DataFusion.

The script requires Docker to be available because it uses the Docker image `ghcr.io/scalytics/tpch-docker` to run
the TPC-H data generator.

Data can be generated as a single Parquet file per table by specifying `--partitions 1`. 

Data will be generated into a `data` directory in the current working directory.

```shell
python tpchgen.py generate --scale-factor 1 --partitions 1
python tpchgen.py convert --scale-factor 1 --partitions 1
```

Data can be generated as multiple Parquet files per table by specifying `--partitions` greater than one. 

```shell
python tpchgen.py generate --scale-factor 1000 --partitions 64
python tpchgen.py convert --scale-factor 1000 --partitions 64
```