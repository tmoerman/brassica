#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting megacell inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.aertslab.grnboost.util.ElbowAdder \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  --conf spark.eventLog.enabled=true \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/megacell/out/stumps.250.cells.100000.2017-05-16T23h11/part-00000 \
  /media/tmo/data/work/datasets/megacell/out/stumps.250.cells.100000.2017-05-16T23h11.elbows
