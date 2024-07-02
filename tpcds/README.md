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

# TPC-DS

## Generating TPC-DS data with Spark

Databricks provides tooling for generating TPC-DS datasets in a Spark cluster:

[https://github.com/databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf)

## Generating TPC-DS data without Spark

For local development and testing, we provide a Python script to generate TPC-DS CSV data and convert it into Parquet,
using DataFusion.

Download the TPC-DS data generator (tpc-ds-tool.zip) from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
and place in this directory.

Note that the TPC-DS generator no longer compiles on modern gcc versions so we need to use a Docker container.

## Build Image

```shell
docker build -t datafusion-benchmarks/tpcdsgen .
```

## Generate Data

Run the Docker container in interactive mode.

```shell
docker run -it -v `pwd`:/data datafusion-benchmarks/tpcdsgen
```

Run the script to generate CSV data.

```shell
./gen.sh
```

## Convert the CSV data to Parquet

TBD

