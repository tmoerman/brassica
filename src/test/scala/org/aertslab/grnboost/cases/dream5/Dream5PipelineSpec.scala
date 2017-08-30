package org.aertslab.grnboost.cases.dream5

import org.aertslab.grnboost.Specs.Server
import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.{GRNBoost, GRNBoostSuiteBase, XGBoostRegressionParams}

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
      nrRounds = Some(25),
      boosterParams = boosterParams)

  behavior of "ScenicPipeline on DREAM5"

  it should "run on the ecoli data set" taggedAs Server in {
    val (dataFile, tfFile) = network(3)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          targetGenes = Set("G666"),
          params = params)

    println(params)

    result.show
  }

}