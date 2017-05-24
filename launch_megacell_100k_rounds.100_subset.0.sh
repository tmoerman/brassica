#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "submitting megacell inference job"
$SPARK_HOME/bin/spark-submit \
  --class org.aertslab.grnboost.cases.megacell.MegacellInferenceFromSubSet \
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
  /media/tmo/data/work/datasets/megacell/out/cell.subsets/100k/nr.rounds.100/megacell.subset.100000.cells.0.txt \
  0 \
  100 \
  11 \
  8