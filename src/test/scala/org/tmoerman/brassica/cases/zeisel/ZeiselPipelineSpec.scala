package org.tmoerman.brassica.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.DataReader._
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Zeisel"

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7
    //"gamma" -> 2
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams)

  val zeiselMrna = props("zeisel")
  val zeiselFiltered = props("zeiselFiltered")

  val mouseTFs = props("mouseTFs")

  it should "run the embarrassingly parallel pipeline from raw" in {
    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna)

    val result =
      ScenicPipeline
        .inferRegulations(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show
  }

  it should "run on a slice of the cells" in {
    val TFs = readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna).slice(0 until 1000)

    val result =
      ScenicPipeline
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

    val expressionByGene = readText(spark, zeiselFiltered)

    val result =
      ScenicPipeline
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