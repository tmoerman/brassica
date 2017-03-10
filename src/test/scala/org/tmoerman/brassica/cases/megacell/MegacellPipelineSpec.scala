package org.tmoerman.brassica.cases.megacell

import breeze.linalg.CSCMatrix
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.TimeUtils
import org.tmoerman.brassica.{RegressionParams, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class MegacellPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline"

  val params =
    RegressionParams(
      normalize = false,
      nrRounds = 10,
      boosterParams = Map(
        "seed" -> 777
        //"silent" -> 1
      ))

  it should "run embarassingly parallel pipeline" in {
    val cellTop = Some(10000)

    val targetTop = 25

    val genes = MegacellReader.readGeneNames(megacell).get

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
    }

    val (_, duration2) = TimeUtils.profile {
      val result = MegacellPipeline
        .apply(
          spark,
          params = params,
          hdf5Path = megacell,
          parquetPath = megacellParquet + "_10k",
          candidateRegulators = MegacellReader.readTFs(mouseTFs),
          targets = genes.take(targetTop),
          cellTop = cellTop,
          nrPartitions = Some(12))
        .collect()
    }

    println(duration1.toMinutes, duration2.toMinutes)

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

  "toDMatrix" should "work correctly" in {

    val builder = new CSCMatrix.Builder[Int](rows = 10, cols = 4)

    builder.add(0, 0, 6)
    builder.add(1, 1, 7)
    builder.add(2, 2, 8)
    builder.add(3, 3, 9)

    val csc = builder.result

    val sliced = csc(0 until 10, Seq(1, 2))
    val dense = sliced.toDenseMatrix

    val arrSliced = sliced.activeValuesIterator.toList

    // TODO 1 of these is obviously wrong!!
    println(sliced.rows, sliced.cols, sliced.activeValuesIterator.toList)
    println(dense.rows, dense.cols, dense.data.toList)

    val dm = MegacellPipeline.toDMatrix(sliced)
    //dm.setLabel(Array(1, 1, 1, 1, 0, 0, 0, 0, 0, 0))

    println(dm.toString)

    dm.delete()
  }

  "calculating the regulator genes" should "work correctly" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    val candidateRegulators = MegacellReader.readTFs(mouseTFs)

    val regulators = ScenicPipeline.toRegulatorGlobalIndexMap(genes, candidateRegulators)

    regulators.size shouldBe 1607
  }

}