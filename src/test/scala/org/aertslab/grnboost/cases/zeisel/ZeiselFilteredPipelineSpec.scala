package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.{GRNBoost, GRNBoostSuiteBase, XGBoostRegressionParams}
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.Specs.Server
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
      nrRounds = Some(7),
      boosterParams = boosterParams)

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" taggedAs Server in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val expressionByGene = readExpressionsByGene(spark, zeiselFiltered)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targetGenes = Set("Sox10"),
          params = params)
        .cache

    println(params)

    withRegularizationLabels(result, params)
      .sort($"gain".desc)
      .show(100)
  }

}