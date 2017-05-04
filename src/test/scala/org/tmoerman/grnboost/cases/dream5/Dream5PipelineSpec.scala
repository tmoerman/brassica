package org.tmoerman.grnboost.cases.dream5

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.{XGBoostRegressionParams, GRNBoost, GRNBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Dream5PipelineSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7
    // "gamma" -> 2
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  behavior of "ScenicPipeline on DREAM5"

  it should "run on the in silico data set" in {
    fail("fixme")
  }

  it should "run on the ecoli data set" in {
    val (dataFile, tfFile) = network(3)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          targets = Set("G666"),
          params = params)

    println(params)

    result.show
  }

  it should "run on s. aureus" in {
    fail("fixme")
  }

  it should "run on s. cerevisiae" in {
    fail("fixme")
  }

}