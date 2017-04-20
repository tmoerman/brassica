package org.tmoerman.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.util.PropsReader.props
import org.tmoerman.grnboost.{XGBoostRegressionParams, GRNBoost, GRNBoostSuiteBase}

import org.tmoerman.grnboost.cases.DataReader._

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredPipelineSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Zeisel"

  val zeiselFiltered = props("zeiselFiltered")

  val mouseTFs = props("mouseTFs")

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams)

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" in {
    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = readExpression(spark, zeiselFiltered)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show
  }

}
