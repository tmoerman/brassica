package org.aertslab.grnboost.cases.megacell

import org.scalatest.FlatSpec
import org.aertslab.grnboost.{GRNBoost, XGBoostRegressionParams, GRNBoostSuiteBase}

import org.aertslab.grnboost.cases.DataReader._

/**
  * @author Thomas Moerman
  */
class MegacellBenchmark extends FlatSpec with GRNBoostSuiteBase {

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
    XGBoostRegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  "the Megacell emb.par pipeline from parquet" should "run" in {
    val TFs = readTFs(mouseTFs).toSet

    // FIXME

//    val result =
//      ScenicPipeline
//        .inferRegulations(
//          spark,
//          hdf5 = megacell,
//          parquet = path,
//          candidateRegulators = TFs,
//          targets = Set("Gad1"),
//          params = params)
//
//    println(params)
//
//    result.show()
  }

}