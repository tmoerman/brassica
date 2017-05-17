package org.tmoerman.grnboost.cases.megacell

import java.io.File

import org.scalatest.FlatSpec
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.util.IOUtils._

import scala.io.Source
import scala.util.Try

/**
  * @author Thomas Moerman
  */
class MegacellSubSetMakerSpec extends FlatSpec {

  "it" should "work for Megacell 100K - 100 boosting rounds" in {
    val wd = "/media/tmo/data/work/datasets/megacell/out/cell.subsets/100k/nr.rounds.100/"

    doit(
      excludedFile = wd + "exclude.txt",
      nrCellsTotal = MegacellReader.MEGACELL_CELL_COUNT,
      nrCellsPerSet = 100000,
      outDir = wd)
  }

  "it" should "work for Megacell 100K - 250 boosting rounds" in {
    val wd = "/media/tmo/data/work/datasets/megacell/out/cell.subsets/100k/nr.rounds.250/"

    doit(
      excludedFile = wd + "exclude.txt",
      nrCellsTotal = MegacellReader.MEGACELL_CELL_COUNT,
      nrCellsPerSet = 100000,
      outDir = wd)
  }

  def doit(excludedFile: String,
           nrCellsTotal: Int,
           nrCellsPerSet: Int,
           outDir: String,
           seed: Seed = DEFAULT_SEED): Unit = {

    val excludeSet: Set[CellIndex] =
      Try(
        Source
          .fromFile(new File(excludedFile))
          .getLines
          .filterNot(_.isEmpty)
          .map(_.trim.toInt)
          .toSet)
        .getOrElse(Set.empty)

    println(s"excluding ${excludeSet.size}")

    val totalSet = (0 until nrCellsTotal).toSet

    val subSets =
      random(seed)
        .shuffle(totalSet diff excludeSet)
        .sliding(nrCellsPerSet, nrCellsPerSet)

    val subSetSizes = subSets.map(_.size).mkString(", ")

    println(s"generating subsets of sizes: ($subSetSizes)")

    subSets
      .zipWithIndex
      .foreach{ case (subSet, idx) =>
        val fileName = s"$outDir/megacell.subset.$nrCellsPerSet.cells.$idx.txt"

        print("writing subset $idx to $fileName... ")

        writeToFile(fileName, subSet.toSeq.sorted.mkString("\n"))

        println("done")
      }

  }

}