package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.megacell.MegacellReader._

import org.apache.spark.ml.linalg.BreezeMLConversions._
import scala.collection.JavaConversions._

/**
  * @author Thomas Moerman
  */
class MegacellReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Megacell reader"

  it should "parse the CSC h5 file correctly" in {
    val vectors = MegacellReader.readCSCMatrixRevised(megacell, limit = Some(10)).get

    val df = spark.createDataFrame(vectors.map(v => Row(v.ml)), StructType(FEATURES_STRUCT_FIELD :: Nil))

    df.show()
  }

  it should "parse the gene list correctly" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    genes.take(5) shouldBe List("Xkr4", "Gm1992", "Gm37381", "Rp1", "Rp1")

    println(genes.take(20).mkString(", "))
  }

}