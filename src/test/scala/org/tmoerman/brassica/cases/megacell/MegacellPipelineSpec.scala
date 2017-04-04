package org.tmoerman.brassica.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.TimeUtils
import org.tmoerman.brassica.{XGBoostRegressionParams, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class MegacellPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Megacell"

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  it should "run the embarrassingly parallel pipeline on top 10k" in {
    val TFs = MegacellReader.readTFs(mouseTFs).toSet

    // TODO slice the input Dataset

    val result =
      MegacellPipeline
        .apply(
          spark,
          hdf5 = megacell,
          parquet = megacellColumnsParquet + "_10k",
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show()
  }

  it should "run the emembarrassingly parallel pipeline on top 1.3m" in {
    val TFs = MegacellReader.readTFs(mouseTFs).toSet

    val result =
      MegacellPipeline
        .apply(
          spark,
          hdf5 = megacell,
          parquet = megacellColumnsParquet + "_full",
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params)

    println(params)

    result.show()
  }

  it should "compare embarassingly parallel pipeline" in {
    val cellTop = Some(10000)

    val targetTop = 100

    val genes = MegacellReader.readGeneNames(megacell).get

    /*
    val (_, duration1) = TimeUtils.profile {
      val result = MegacellPipeline
        .apply(
          spark,
          params = params,
          hdf5Path = megacell,
          parquetPath = megacellParquet + "_10k",
          candidateRegulators = MegacellReader.readTFs(mouseTFs),
          targets = genes.take(targetTop),
          cellTop = cellTop,
          nrPartitions = Some(1))
        .collect()
    }*/

    val TFs = MegacellReader.readTFs(mouseTFs).toSet

    val (_, duration2) = TimeUtils.profile {
      val result = MegacellPipeline
        .apply(
          spark,
          hdf5 = megacell,
          parquet = megacellColumnsParquet + "_10k",
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params,
          nrPartitions = None)

      result.show()
    }

    // println(duration1.toSeconds, duration2.toSeconds)
    println(duration2.toSeconds)

    println(params)
  }

  it should "run XGBoostSpark" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    // val rows = MegacellReader.readRows(megacell, Can

//    val (df, genes) = MegacellReader.apply(spark, megacell).get // TODO read Parquet better ???
//
//    val limit = Some(10000)
//
//    val vectors = MegacellReader.readCSCMatrixRevised(megacell, limit).get
//
//    val rows = vectors.map(v => Row(v.ml))
//    +
//      +    val df = spark.createDataFrame(rows, StructType(FEATURES_STRUCT_FIELD :: Nil))
//    +
//      +    val genes = MegacellReader.readGeneNames(megacell).get
//
//    val TFs = MegacellReader.readTFs(mouseTFs)
//
//    val (grn, info) =
//      ScenicPipeline.apply(
//        spark,
//        df,
//        genes,
//        params = params,
//        nrRounds = 10,
//        candidateRegulators = TFs,
//        -        targets = List("Xkr4", "Rp1", "Sox17", "Lypla1", "Oprk1", "St18"),
//        +        //targets = List("Xkr4", "Rp1", "Sox17", "Lypla1", "Oprk1", "St18"),
//          +        targets = List("Gad1"),
//        nrWorkers = Some(1)
//      )
//
//    grn.show()
//
//    println(info.mkString("\n"))
  }

}