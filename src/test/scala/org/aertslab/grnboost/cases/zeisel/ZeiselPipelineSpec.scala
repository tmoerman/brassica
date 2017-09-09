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
      nrRounds = Some(250),
      boosterParams = boosterParams)

  val zeiselMrna     = props("zeisel")
  val zeiselFiltered = props("zeiselFiltered")
  val mouseTFs       = props("mouseTFs")
  val targets        = Set("Gad1")

  it should "run the embarrassingly parallel pipeline from raw" taggedAs Slow in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna)

    val (_, updatedParams) =
      GRNBoost
        .updateConfigsWithEstimation(
          expressionByGene,
          candidateRegulators = TFs,
          targetGenes = targets,
          params = params)

    println(s"estimated nr of rounds: ${updatedParams.nrRounds}")

    val result =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targetGenes = targets,
          params = updatedParams)

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
          targetGenes = targets,
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
          targetGenes = targets,
          params = params)

    println(params)

    result.sort($"gain".desc).show
  }

  val zeiselParquet = props("zeiselParquet")

}