#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "Launching cfg-run"
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
  -i  /media/tmo/data/work/datasets/macosko/in/allEsetMR.tsv \
  -tf /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  -o  /media/tmo/data/work/datasets/macosko/out/8ab4224/full \
  -skip 1 \
  --regularized \
  --truncate 100000