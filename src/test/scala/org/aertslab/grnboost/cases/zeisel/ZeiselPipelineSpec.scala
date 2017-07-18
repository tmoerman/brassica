package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost._
import DataReader._
import org.aertslab.grnboost.util.PropsReader.props
import org.scalatest.tagobjects.Slow

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Zeisel"

  val boosterParams = Map(
    "seed"      -> 777,
    "eta"       -> 0.1,
    "max_depth" -> 3,
    "silent"    -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 250,
      boosterParams = boosterParams)

  val zeiselMrna     = props("zeisel")
  val zeiselFiltered = props("zeiselFiltered")
  val mouseTFs       = props("mouseTFs")

  it should "run the embarrassingly parallel pipeline from raw" taggedAs Slow in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna)

    val estimations =
      GRNBoost
        .roundsEstimations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)
        .cache

    estimations.show

    import org.apache.spark.sql.functions._

    val estimatedNrRounds = estimations.select(max("rounds")).first.getInt(0)

    println(s"estimated nr of rounds: $estimatedNrRounds")

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params.copy(nrRounds = estimatedNrRounds))

    println(params)

    result
      .sort($"gain".desc)
      .show(250)
  }

  it should "run on a slice of the cells" taggedAs Slow in {
    val TFs = readRegulators(mouseTFs).toSet

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

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" taggedAs Slow in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val expressionByGene = readExpressionsByGene(spark, zeiselFiltered)

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.sort($"gain".desc).show
  }

  val zeiselParquet = props("zeiselParquet")

}