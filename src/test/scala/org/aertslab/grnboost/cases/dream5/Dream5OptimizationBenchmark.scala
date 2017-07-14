package org.aertslab.grnboost.cases.dream5

import java.lang.Runtime.getRuntime

import org.aertslab.grnboost.Specs.Server
import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.{GRNBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5OptimizationBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val writePath = props("dream5Optimization")

  "Dream5 hyperparam optimization" should "run" taggedAs Server ignore {
    Seq(3).foreach(optimizeHyperParams)
  }

  val optimizationParams: XGBoostOptimizationParams =
    XGBoostOptimizationParams(
      nrTrials = 10000 / 88,
      // nrBatches = 88,
      onlyBestTrial = false)

  val nrCores = getRuntime.availableProcessors

  val TARGETS = "G3" :: Nil

  private def optimizeHyperParams(idx: Int): Unit = {
    import spark.implicits._

    val (dataFile, tfFile) = network(idx)

    val (expressionsByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val hyperParamsLossDS =
      GRNBoost
        .optimizeHyperParams(
          expressionsByGene,
          candidateRegulators = tfs.toSet,
          params = optimizationParams,
          targets = TARGETS.toSet,
          nrPartitions = Some(nrCores))
        .cache()

    hyperParamsLossDS
      .sort($"target", $"loss".desc)
      .show()

    hyperParamsLossDS
      .write
      .mode(Overwrite)
      .parquet(writePath + s"network${idx}_small")
  }

  // http://stackoverflow.com/questions/31789939/calculate-the-standard-deviation-of-grouped-data-in-a-spark-dataframe
  "adding a standardized loss column" should "work" taggedAs Server in {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ds = spark.read.parquet(writePath + s"network3_small").cache

    val statsByTarget =
      ds
        .groupBy($"target")
        .agg(
          stddev("loss").as("stddev_loss"),
          mean("loss").as("mean_loss"))

    statsByTarget.show

    ds
      .join(statsByTarget, ds("target") === statsByTarget("target"), "outer")
      .withColumn("std_loss", ($"loss" - $"mean_loss") / $"stddev_loss")
      .sort($"std_loss")
      .show

  }

}