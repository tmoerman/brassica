package org.tmoerman.brassica.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{RegressionParams, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

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
    RegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams)

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" in {
    val TFs = ZeiselFilteredReader.readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    val result =
      ScenicPipeline
        .apply(
          expressionByGene,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params.copy(showCV = true))

    println(params)

    result.show
  }

}
