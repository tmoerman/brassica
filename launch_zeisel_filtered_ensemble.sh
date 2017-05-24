#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting Zeisel (filtered) inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.aertslab.grnboost.cases.zeisel.ZeiselInferenceEnsemble \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  --conf spark.eventLog.enabled=true \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/zeisel/expression_sara_filtered.txt \
  /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  /media/tmo/data/work/datasets/zeisel/out/filtered \
  88 \
  1 \
  20 \
  10