package org.tmoerman.brassica.cases.megacell

import org.scalatest.FlatSpec
import org.tmoerman.brassica.{RegressionParams, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class MegacellBenchmark extends FlatSpec with XGBoostSuiteBase {

  val path = "/media/tmo/data/work/datasets/megacell_parquet_full"
  //val path = megacellColumnsParquet + "_10k"

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2,
    "nthread" -> 1
  )

  val params =
    RegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  "the Megacell emb.par pipeline from parquet" should "run" in {
    val TFs = MegacellReader.readTFs(mouseTFs).toSet

    val result =
      MegacellPipeline
        .apply(
          spark,
          hdf5 = megacell,
          parquet = path,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show()
  }

}