package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class MegacellReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Megacell reader"

  it should "parse the CSC matrix correctly" ignore {
    val csc = MegacellReader.readCSCMatrix(megacell).get

    (csc.rows, csc.cols) shouldBe (1300000, 27000) // approximately, fix this
  }

  it should "parse the gene list correctly" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    genes.take(5) shouldBe List("Xkr4", "Gm1992", "Gm37381", "Rp1", "Rp1")

    println(genes.take(20).mkString(", "))
  }

}