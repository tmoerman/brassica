package org.tmoerman.brassica.cases.megacell

import breeze.linalg._
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.TimeUtils
// import breeze.numerics._

/**
  * @author Thomas Moerman
  */
class MegacellReaderSpec extends FlatSpec with Matchers {

  behavior of "Megacell reader"

  it should "read CSC matrix, limit 10" in {
    val (nrCells, nrGenes) = MegacellReader.readDimensions(megacell).get

    val csc: CSCMatrix[Int] = MegacellReader.readCSCMatrix(megacell, limit = Some(10)).get

    val col1 = csc(0 until nrCells, 13).flatten()

    col1.toArray.take(10) shouldBe Array(1, 1, 0, 0, 2, 1, 1, 1, 0, 0)
  }

  it should "read CSC matrix, limit 10k" in {
    val (nrCells, nrGenes) = MegacellReader.readDimensions(megacell).get

    val limit = Some(1000)

    val (csc, duration) = TimeUtils.profile {
      MegacellReader.readCSCMatrix(megacell, limit = limit).get
    }

    println(s"reading ${limit.get} Megacell entries took ${duration.toSeconds} seconds")

    val col1 = csc(0 until nrCells, 13).flatten()

    col1.toArray.take(10) shouldBe Array(1, 1, 0, 0, 2, 1, 1, 1, 0, 0)
  }

  it should "parse the gene list correctly" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    genes.take(5) shouldBe List("Xkr4", "Gm1992", "Gm37381", "Rp1", "Rp1")

    println(genes.take(20).mkString(", "))
  }

}