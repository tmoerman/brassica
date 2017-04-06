package org.tmoerman.brassica.cases.dream5

import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5OptimizationBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val writePath = props("dream5Optimization")

  "Dream5 hyperparam optimization" should "run" in {
    Seq(3).foreach(optimizeHyperParams)
  }

  val optimizationParams: XGBoostOptimizationParams =
    XGBoostOptimizationParams(
      nrTrials = 500,
      parallel = false,
      onlyBestTrial = false)

  val TF_50     = (1 to 50).map(i => s"G$i").toList
  val NORMAL_50 = (500 to 550).map(i => s"G$i").toList
  val TARGETS   = TF_50 ::: NORMAL_50

  private def optimizeHyperParams(idx: Int): Unit = {
    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val optimizedHyperParamsDS =
      ScenicPipeline
        .optimizeHyperParams(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = optimizationParams,
          targets = TARGETS.toSet)
        .cache()

    optimizedHyperParamsDS
      .show()

    optimizedHyperParamsDS
      .write
      .mode(Overwrite)
      .parquet(writePath + s"network$idx")
  }

}