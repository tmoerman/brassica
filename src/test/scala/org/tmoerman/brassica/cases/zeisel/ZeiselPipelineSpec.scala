package org.tmoerman.brassica.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica._

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
    "colsample_bytree" -> 0.7,
    "gamma" -> 2
  )

  val params =
    RegressionParams(
      normalize = true,
      nrRounds = 50,
      boosterParams = boosterParams)

  it should "run the embarrassingly parallel pipeline from raw" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val result =
      ZeiselPipeline
        .apply(
          spark,
          file = zeiselMrna,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show
  }

  it should "run the emb.par pipeline on filtered (cfr. Sara) zeisel data" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val result =
      ZeiselFilteredPipeline
        .apply(
          spark,
          file = zeiselFiltered,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show
  }

  it should "run the old Spark scenic pipeline" in {
    val (df, genes) = ZeiselReader.fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val (grn, info) =
      ScenicPipeline_OLD.apply(
        spark,
        df,
        genes,
        nrRounds = 25,
        candidateRegulators = TFs,
        params = boosterParams,
        targets = Set("Gad1"),
        nrWorkers = Some(8))

    grn.show()

    println(info.mkString("\n"))
  }

}