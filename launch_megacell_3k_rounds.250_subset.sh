#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting megacell inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.tmoerman.grnboost.cases.megacell.MegacellInferenceFromSubSet \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  --conf spark.eventLog.enabled=true \
  ./target/scala-2.11/GRNBoost.jar \
  /media/tmo/data/work/datasets/megacell/parquet_full \
  /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  /media/tmo/data/work/datasets/megacell/out \
  /media/tmo/data/work/datasets/megacell/out/cell.subsets/3k/nr.rounds.250/subset.3k.from.exclude.txt \
  3k \
  250 \
  88 \
  1