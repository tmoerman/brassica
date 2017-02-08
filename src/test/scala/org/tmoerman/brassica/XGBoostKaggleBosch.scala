package org.tmoerman.brassica

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoost => SparkXGBoost}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.lit
import org.scalatest.{FlatSpec, Matchers}

/**
  * Based on: https://www.elenacuoco.com/2016/10/10/scala-spark-xgboost-classification/
  *
  * Issue: https://github.com/dmlc/xgboost/issues/1827
  */
class XGBoostKaggleBosch extends FlatSpec with DataFrameSuiteBase with Matchers {

  override def conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("XGBoost-Spark Kaggle Bosch")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .registerKryoClasses(Array(classOf[Booster]))

  behavior of "xgboost"

  it should "work" in {

    val path = "src/test/resources/bosch/"

    val trainSet =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", true)
        .csv(path + "train_numeric.csv")
        .cache

    lazy val testSet =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", true)
        .csv(path + "/test_numeric.csv")
        .cache

    val r0 = trainSet.take(1).apply(0)

    // assemble the features

    val df = trainSet.na.fill(0).sample(withReplacement = true, fraction = 0.7, seed = 10)
    val df_test = testSet.na.fill(0)

    val featureColumns =
      df
        .columns
        .filter(! _.contains("Id"))       // id is not a predictive
        .filter(! _.contains("Response")) // response is not a predictive feature

    val assembler =
      new VectorAssembler()
        .setInputCols(featureColumns) // filtered header: id and response are removed.
        .setOutputCol("features")     // puts the feature column values into a SparseVector.

    val train_DF0 = assembler.transform(df)
    val test_DF0 = assembler.transform(df_test)

    // prep for xgboost

    val train =
      train_DF0
        .withColumn("label", df("Response").cast("double")) // add "label" column with the response from the original DF
        .select("label", "features")                        // select label (value) and features (sparse vector)

    val test =
      test_DF0
        .withColumn("label", lit(1.0)) // TODO why ???
        .withColumnRenamed("Id", "id")
        .select("id", "label", "features")

    val t0 = train.take(1).apply(0)

    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = 0)

    // number of iterations
    val numRound = 10
    val numWorkers = 4

    // training parameters
    val paramMap = Map(
      "eta"               -> 0.023f,
      "max_depth"         -> 10,
      "min_child_weight"  -> 3.0,
      "subsample"         -> 1.0,
      "colsample_bytree"  -> 0.82,
      "colsample_bylevel" -> 0.9,
      "base_score"        -> 0.005,
      "eval_metric"       -> "auc",
      "seed"              -> 49,
      "silent"            -> 1,
      "objective"         -> "binary:logistic")

    val resultModel =
      SparkXGBoost
        .trainWithDataFrame(
          trainingData,
          paramMap,
          round = numRound,
          nWorkers = numWorkers,
          useExternalMemory = true)

    val predictions =
      resultModel
        .setExternalMemory(true)
        .transform(testData)
        .select("label", "probabilities")

    predictions
      .write
      .save(path+"preds.parquet")

  }

}
