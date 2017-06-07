package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.{XGBoostRegressionParams, GRNBoost, GRNBoostSuiteBase}

import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost._

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
    "max_depth" -> 3,
    "subsample" -> 0.25,
    "colsample_bytree" -> 0.25
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 7,
      boosterParams = boosterParams)

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val expressionByGene = readExpressionsByGene(spark, zeiselFiltered)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Sox10"),
          params = params)
        .cache

    println(params)

    result
      .addElbowGroups(params)
      .sort($"gain".desc)
      .show(100)
  }

}