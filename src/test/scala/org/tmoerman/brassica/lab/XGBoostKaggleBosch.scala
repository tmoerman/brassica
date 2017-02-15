package org.tmoerman.brassica.lab

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoost => SparkXGBoost}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
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

  it should "work" ignore {

    val pathIn  = "src/test/resources/bosch/"
    val pathOut = pathIn + "out/"

    val trainSet =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", true)
        .csv(pathIn + "train_numeric.csv")
        .cache

    lazy val testSet =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", true)
        .csv(pathIn + "/test_numeric.csv")
        .cache

    // assemble the features

    val df = trainSet.na.fill(0).sample(withReplacement = true, fraction = 0.7, seed = 10) // why sample this?

    val df_test = testSet.na.fill(0)

    val featureColumns: Array[String] =
      df
        .columns
        .filter(! _.contains("Id"))       // id is not a predictive
        .filter(! _.contains("Response")) // response is not a predictive feature

    val assembler =
      new VectorAssembler()
        .setInputCols(featureColumns) // filtered header: id and response are removed.
        .setOutputCol("features")     // puts the feature column values into a SparseVector.

    // prep for xgboost

    val train =
      assembler
        .transform(df)
        .withColumn("label", df("Response").cast("double")) // add "label" column with the response from the original DF
        .select("label", "features")                        // select label (value) and features (sparse vector)

    val test =
      assembler
        .transform(df_test)
        .withColumn("label", lit(1.0)) // TODO why ???
        .withColumnRenamed("Id", "id")
        .select("id", "label", "features")

    val t0 = train.take(1).apply(0)

    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = 0)

    // number of iterations
    val numRound   = 10
    val numWorkers = 4

    // training parameters
    val paramMap = Map(
      "eta"               -> 0.023f, // (default: 0.3) step size shrinkage used in update to prevents overfitting
      "max_depth"         -> 10,     // (default: 6) maximum depth of a tree
      "min_child_weight"  -> 3.0,    // (default: 1) minimum sum if instance weight
      "subsample"         -> 1.0,    // (default: 1 = ALL) subsample ratio of the training instance
      "colsample_bytree"  -> 0.82,   // (default: 1 = ALL) subsample ratio of columns when constructing each tree.
      "colsample_bylevel" -> 0.9,    // (default: 1 = ALL) subsample ratio of columns for each split, in each level
      "base_score"        -> 0.005,  // (default: 0.5) the initial prediction score of all instances, global bias
      "eval_metric"       -> "auc",
      "seed"              -> 49,
      "silent"            -> 1,
      "objective"         -> "binary:logistic") // Specify the learning task and the corresponding learning objective

    val resultModel =
      SparkXGBoost
        .trainWithDataFrame(
          trainingData,
          paramMap,
          round    = numRound,
          nWorkers = numWorkers,
          useExternalMemory = true) // TODO try with false

    val predictions: DataFrame =
      resultModel
        .setExternalMemory(true)
        .transform(testData)
        .select("label", "probabilities")

    predictions.show(3)

    predictions
      .write
      .save(pathOut + "preds.parquet")

    //prediction on test set for submission file
    val submission = resultModel.setExternalMemory(true).transform(test).select("id", "probabilities")
    submission.show(10)
    submission.write.save(pathOut + "submission.parquet")

    spark.stop()

  }

}
