#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting macosko (sampled) inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.tmoerman.grnboost.cases.macosko.MacoskoInference \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/macosko/in/allEsetMR.tsv \
  /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  /media/tmo/data/work/datasets/macosko/out/full \
  88 \
  1 \
  50