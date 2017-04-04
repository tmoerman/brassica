package org.tmoerman.brassica.cases.dream5

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline, XGBoostOptimizationParams, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Dream5OptimizationBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  "Dream5 hyperparam optimization" should "run" in {
    Seq(3).foreach(optimizeHyperParams)
  }

  val optimizationParams: XGBoostOptimizationParams = XGBoostOptimizationParams(nrTrials = 20)

  private def optimizeHyperParams(idx: Int): Unit = {
    println(s"computing network $idx")

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val optimizedHyperParamsDS =
      ScenicPipeline
        .optimizeHyperParams(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = optimizationParams,
          targets = Set("G3", "G10", "G7"),
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
    //.cache()

    optimizedHyperParamsDS
      .show()
  }

}