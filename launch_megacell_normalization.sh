#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting megacell inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.tmoerman.brassica.cases.normalize.NormalizeRegulations \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/megacell/out/part-00000 \
  /media/tmo/data/work/datasets/megacell/norm