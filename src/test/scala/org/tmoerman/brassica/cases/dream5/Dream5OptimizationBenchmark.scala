package org.tmoerman.brassica.cases.dream5

import java.lang.Runtime.getRuntime

import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5OptimizationBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val writePath = props("dream5Optimization")

  "Dream5 hyperparam optimization" should "run" ignore {
    Seq(3).foreach(optimizeHyperParams)
  }

  val optimizationParams: XGBoostOptimizationParams =
    XGBoostOptimizationParams(
      nrTrialsPerBatch = 10000 / 88,
      nrBatches = 88,
      onlyBestTrial = false)

  val nrCores = getRuntime.availableProcessors

//  val TF_50     = (  1 to   0 + (nrCores / 2)).map(i => s"G$i").toList
//  val NORMAL_50 = (501 to 500 + (nrCores / 2)).map(i => s"G$i").toList
//  val TARGETS   = TF_50 ::: NORMAL_50

  val TARGETS = "G3" :: Nil

  private def optimizeHyperParams(idx: Int): Unit = {
    import spark.implicits._

    val (dataFile, tfFile) = network(idx)

    val (expressionsByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val optimizedHyperParamsDS =
      ScenicPipeline
        .optimizeHyperParams(
          expressionsByGene,
          candidateRegulators = tfs.toSet,
          params = optimizationParams,
          targets = TARGETS.toSet,
          nrPartitions = Some(nrCores))
        .cache()

    optimizedHyperParamsDS
      .sort($"target", $"loss".desc)
      .show()

    optimizedHyperParamsDS
      .write
      .mode(Overwrite)
      .parquet(writePath + s"network${idx}_small")
  }

  // http://stackoverflow.com/questions/31789939/calculate-the-standard-deviation-of-grouped-data-in-a-spark-dataframe
  "adding a standardized loss column" should "work" in {

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