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
    