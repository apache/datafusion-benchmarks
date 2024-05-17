<!--
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

# DataFusion Comet Benchmark derived from TPC-H and TPC-DS

Follow the [Comet Installation](https://datafusion.apache.org/comet/user-guide/installation.html) guide to download or 
create a Comet JAR file.

```shell
export COMET_JAR=spark/target/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar

$SPARK_HOME/bin/spark-submit \
    --master "local[*]" \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    tpcbench.py \
    --benchmark tpch \
    --data /Users/andy/Data/sf1-parquet/ \
    --queries ../../tpch/queries/sf\=1/
```
    