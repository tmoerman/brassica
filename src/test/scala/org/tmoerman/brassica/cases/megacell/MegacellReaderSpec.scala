package org.tmoerman.brassica.cases.megacell

import breeze.linalg._
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.megacell.MegacellReader.{DoubleSparseVector, IntSparseVector}
import org.tmoerman.brassica.util.TimeUtils
import breeze.numerics._

import org.apache.spark.ml.linalg.BreezeMLConversions._

/**
  * @author Thomas Moerman
  */
class MegacellReaderSpec extends FlatSpec with Matchers {

  val COL_13 = Array(1, 1, 0, 0, 2, 1, 1, 1, 0, 0)

  behavior of "Megacell reader"

  it should "read sparse vectors, limit 1k" in {
    val limit = Some(10)

    val vectors = MegacellReader.readRows(megacell, DoubleSparseVector, cellTop = limit).get

    val col13 = vectors.map(_.ml.apply(13)) // remove .ml for a nice scalac error

    col13.toArray shouldBe COL_13
  }

  it should "read sparse vectors, limited and restricted" in {
    val limit = Some(10)

    val predicate = Some(Set(7, 37))

    val vectors =
      MegacellReader
        .readRows(megacell, DoubleSparseVector, cellTop = limit, genePredicate = predicate)
        .get
        .map(_.ml)

    val col7  = vectors.map(_.apply(7))
    val col13 = vectors.map(_.apply(13))
    val col37 = vectors.map(_.apply(37))

    col7.toArray shouldBe  Array(0, 0, 0, 0, 2, 0, 2, 0, 1, 1)
    col13.toArray shouldBe Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    col37.toArray shouldBe Array(1, 0, 2, 2, 0, 0, 0, 0, 0, 0)
  }

  it should "read the matrix dimensions" in {
    MegacellReader.readDimensions(megacell).get shouldBe (1300774,27998)
  }

  it should "read CSC matrix, limit 10" in {
    val limit = Some(10)

    val (_, nrGenes) = MegacellReader.readDimensions(megacell).get

    val csc: CSCMatrix[Int] = MegacellReader.readCSCMatrix(megacell, cellTop = limit).get

    csc.rows shouldBe limit.get
    csc.cols shouldBe nrGenes

    val col13 = csc(0 until limit.get, 13).flatten()

    col13.toArray.take(10) shouldBe COL_13
  }

  it should "read CSC matrix, limit 1k" in {
    val limit = Some(1000)

    val (_, nrGenes) = MegacellReader.readDimensions(megacell).get

    val (csc, duration) = TimeUtils.profile {
      MegacellReader.readCSCMatrix(megacell, cellTop = limit).get
    }

    csc.rows shouldBe limit.get
    csc.cols shouldBe nrGenes

    println(s"reading ${limit.get} Megacell entries took ${duration.toSeconds} seconds")

    val col13 = csc(0 until limit.get, 13).flatten()

    col13.toArray.take(10) shouldBe COL_13
  }

  it should "parse the gene list correctly" in {
    val genes = MegacellReader.readGeneNames(megacell).get

    genes.take(5) shouldBe List("Xkr4", "Gm1992", "Gm37381", "Rp1", "Rp1")

    println(genes.take(20).mkString(", "))
  }

}