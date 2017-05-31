package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost._
import org.aertslab.grnboost.cases.DataReader._
import org.aertslab.grnboost.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Zeisel"

  val boosterParams = Map(
    "seed" -> 777,
    "eta"              -> 0.1,
    "subsample"        -> 0.8,
    "colsample_bytree" -> 0.25,
    "max_depth"        -> 1,
    "silent" -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 250,
      boosterParams = boosterParams)

  val zeiselMrna = props("zeisel")
  val zeiselFiltered = props("zeiselFiltered")

  val mouseTFs = props("mouseTFs")

  it should "run the embarrassingly parallel pipeline from raw" in {
    import spark.implicits._

    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result
      .sort($"gain".desc)
      .show(250)
  }

  it should "run on a slice of the cells" in {
    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna).slice(0 until 1000)

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

  val zeiselParquet = props("zeiselParquet")

}