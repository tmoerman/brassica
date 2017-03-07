package org.tmoerman.brassica.cases.megacell

import breeze.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.megacell.MegacellReader._
import org.tmoerman.brassica.{ScenicPipeline, XGBoostSuiteBase}

import org.apache.spark.ml.linalg.BreezeMLConversions._
import scala.collection.JavaConversions._

/**
  * @author Thomas Moerman
  */
class MegacellPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline"

  val params = Map(
    "seed" -> 777,
    "silent" -> 1
  )

  it should "run on target subset of the Megacell data set" in {
    // val (df, genes) = MegacellReader.apply(spark, megacell).get // TODO read Parquet better ???

    val limit = Some(10000)

    // val vectors = MegacellReader.readSparseVectors(megacell, limit).get

    val vectors = MegacellReader.readRows(megacell, DoubleSparseVector, cellTop = limit).get

    val rows = vectors.map(v => Row(v.ml))

    val df = spark.createDataFrame(rows, StructType(FEATURES_STRUCT_FIELD :: Nil))

    val genes = MegacellReader.readGeneNames(megacell).get

    val TFs = MegacellReader.readTFs(mouseTFs)

    val (grn, info) =
      ScenicPipeline.apply(
        spark,
        df,
        genes,
        params = params,
        nrRounds = 10,
        candidateRegulators = TFs,
        //targets = List("Xkr4", "Rp1", "Sox17", "Lypla1", "Oprk1", "St18"),
        targets = List("Gad1"),
        nrWorkers = Some(1)
      )

    grn.show()

    println(info.mkString("\n"))
  }

}