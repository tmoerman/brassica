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

    // MegacellScenicPipeline.
  }

}