echo "SBT .jar assembly"
sbt assembly

echo "submitting Spark job"
$SPARK_HOME/bin/spark-submit \
  --class org.tmoerman.brassica.cases.megacell.MegacellInference \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 96g \
  --conf spark.executor.memory=96g \
  --conf spark.driver.maxResultSize=20gb \
  --conf spark.network.timeout=10000000 \
  ./target/scala-2.11/gradinets.jar \
  /media/tmo/data/work/datasets/megacell/parquet_full \
  /home/tmo/work/batiskav/projects/brassica/src/test/resources/TF/mm9_TFs.txt \
  /media/tmo/data/work/datasets/megacell/out \
  100000 \
  ALL \
  11 \
  8 \
  20