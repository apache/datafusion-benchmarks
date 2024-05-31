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

# Apache DataFusion Benchmarks

## Overview

This repository is intended as a central resource for documentation and scripts for running queries derived from the
industry standard TPC-H and TPC-DS benchmarks against DataFusion and its subprojects, as well as against other
open-source query engines for comparison.

TPC-H and TPC-DS both operate on synthetic data, which can be generated at different "scale factors". A scale factor
of 1 means that approximately 1 GB of CSV data is generated, and a scale factor of 1000 means that approximately 1 TB
of data is generated.

## TPC Legal Considerations

It is important to know that TPC benchmarks are copyrighted IP of the Transaction Processing Council. Only members
of the TPC consortium are allowed to publish TPC benchmark results. Fun fact: only four companies have published
official TPC-DS benchmark results so far, and those results can be seen [here](https://www.tpc.org/tpcds/results/tpcds_results5.asp?orderby=dbms&version=3).

However, anyone is welcome to create derivative benchmarks under the TPC's fair use policy, and that is what we are
doing here. We do not aim to run a true TPC benchmark (which is a significant endeavor). We are just running the
individual queries and recording the timings.

Throughout this document and when talking about these benchmarks, you will see the term "derived from TPC-H" or
"derived from TPC-DS". We are required to use this terminology and this is explained in the
[fair-use policy (PDF)](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

DataFusion benchmarks are a Non-TPC Benchmark. Any comparison between official TPC Results with non-TPC workloads is
prohibited by the TPC.

## Data Generation

See the benchmark-specific instructions for generating the CSV data for [TPC-H](tpch) and [TPC-DS](tpcds) and for 
converting that data to Parquet format. Although it is valid to run benchmarks against CSV data, this does not really 
represent how most of the world is running OLAP queries, especially when dealing with large datasets. When benchmarking 
DataFusion and its subprojects, we typically want to be querying Parquet data. Also, we typically do not
want a single file per table, so we also need to repartition the data. The provided scripts take care of this conversion 
and repartitioning.

## Running the Benchmarks with DataFusion

Scripts are available for the following DataFusion projects:

- [DataFusion Python](./runners/datafusion-python)
- [DataFusion Comet](./runners/datafusion-comet)

These benchmarking scripts produce JSON files containing query timings.

## Comparing Results

The Python script [scripts/generate-comparison.py](scripts/generate-comparison.py) can be used to produce charts 
comparing results from different benchmark runs.

For example:

```shell
python scripts/generate-comparison.py file1.json file2.json --labels "Spark" "Comet" --benchmark "TPC-H 100GB"
```

This will create image files in the current directory in PNG format.

## Legal Notices

TPC-H is Copyright © 1993-2022 Transaction Processing Performance Council. The full TPC-H specification in PDF format
can be found [here](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf).

TPC-DS is Copyright © 2021 Transaction Processing Performance Council. The full TPC-DS specification in PDF format
can be found [here](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf).

TPC, TPC Benchmark, TPC-H, and TPC-DS are trademarks of the Transaction Processing Performance Council.
