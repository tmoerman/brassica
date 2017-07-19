#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "Launching inference"
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
  -i  /media/tmo/data/work/datasets/dream5/training\ data/Network\ 3\ -\ E.\ coli/net3_expression_data.transposed.tsv \
  -tf /media/tmo/data/work/datasets/dream5/training\ data/Network\ 3\ -\ E.\ coli/net3_transcription_factors.tsv \
  -o  /media/tmo/data/work/datasets/dream5/grnboost/net3/net3_grnboost_depth3.tsv \
  -p eta=0.01 \
  -p max_depth=3 \
  -p colsample_bytree=0.055 \
  --truncate 100000