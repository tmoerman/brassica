#!/usr/bin/env bash

echo "SBT .jar assembly"
sbt assembly

echo "Launching cfg-run"
$SPARK_HOME/bin/spark-submit \
  --class org.aertslab.grnboost.GRNBoost \
  --master local[*] \
  ./target/scala-2.11/GRNBoost.jar \
  --help