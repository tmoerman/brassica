package org.tmoerman.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.util.PropsReader.props
import org.tmoerman.grnboost.{XGBoostRegressionParams, GRNBoost, GRNBoostSuiteBase}

import org.tmoerman.grnboost.cases.DataReader._
import org.tmoerman.grnboost._

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
      nrRounds = 75,
      boosterParams = boosterParams,
      //metric = FREQ
      metric = GAIN
    )

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" in {
    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = readExpression(spark, zeiselFiltered)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Sox10"),
          params = params)
        .cache

    println(params)

    import org.apache.spark.sql.functions._

    result
      .normalizeBy(avg)
      .show(100)
  }

}