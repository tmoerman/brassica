#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "Launching dry-run"
$SPARK_HOME/bin/spark-submit \
  --class org.aertslab.grnboost.GRNBoost \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  --conf spark.eventLog.enabled=true \
  ./target/scala-2.11/GRNBoost.jar \
  infer \
  -i  src/test/resources/genie3/data.txt \
  -tf src/test/resources/TF/mm9_TFs.txt \
  -o  src/test/resources/genie3/out.txt \
  --transposed --dry-run
