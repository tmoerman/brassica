#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting Macosko (full) inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.tmoerman.grnboost.cases.macosko.MacoskoInference \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  --conf spark.eventLog.enabled=true \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/macosko/in/allEsetMR.tsv \
  /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  /media/tmo/data/work/datasets/macosko/out/stumps_1000/full \
  88 \
  1