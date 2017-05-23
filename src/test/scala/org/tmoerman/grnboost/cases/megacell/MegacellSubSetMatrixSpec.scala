package org.tmoerman.grnboost.cases.megacell

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
class MegacellSubSetMatrixSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val parquet   = "/media/tmo/data/work/datasets/megacell/parquet_full"

  "it" should "work" ignore {
    val subSetDir = "/media/tmo/data/work/datasets/megacell/out/cell.subsets/100k/nr.rounds.100"
    val outDir    = "/media/tmo/data/work/datasets/megacell/out/matrix.subsets/100k/nr.rounds.100"

    doit(parquet, subSetDir, outDir)
  }

  "it" should "work for 3k and 10k" in {

    val subSets =
      Seq(
        "/media/tmo/data/work/datasets/megacell/out/cell.subsets/3k/nr.rounds.250/subset.3k.from.exclude.txt",
        "/media/tmo/data/work/datasets/megacell/out/cell.subsets/10k/nr.rounds.250/subset.10k.from.exclude.txt")
        .map(n => new File(n))

    val outDirs =
      Seq(
        "/media/tmo/data/work/datasets/megacell/out/matrix.subsets/3k/nr.rounds.250/from.exclude",
        "/media/tmo/data/work/datasets/megacell/out/matrix.subsets/10k/nr.rounds.250/from.exclude")

    import spark.implicits._

    val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache

    (subSets zip outDirs)
      .foreach{ case (subSetFile, outDir) => {

        val sliceIndices = Source.fromFile(subSetFile).getLines.filterNot(_.isEmpty).map(_.trim.toInt).toSeq

        val header =
          spark
            .sparkContext
            .parallelize(Seq(s"gene\t${sliceIndices.mkString("\t")}"))

        val rows =
          ds
            .slice(sliceIndices)
            .rdd
            .map(e => s"${e.gene}\t${e.values.toArray.map(_.toInt).mkString("\t")}" )

        (header union rows)
          .repartition(1)
          .saveAsTextFile(s"$outDir/${subSetFile.getName}")

      }}
  }

  def doit(parquet: String,   // megacell parquet file
           subSetDir: String, // read all files from here
           outDir: String,    // write the matrices here
           sliceSize: Int = 10000): Unit = {

    import spark.implicits._

    val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache

    new File(subSetDir)
      .listFiles
      .toList
      .sorted
      .foreach{ subSetFile => try {

        val subSet = Source.fromFile(subSetFile).getLines.filterNot(_.isEmpty).map(_.trim.toInt).toSet

        subSet
          .toList
          .sorted
          .sliding(sliceSize, sliceSize)
          .zipWithIndex
          .foreach{ case (sliceIndices, sliceIdx) => {

            val header =
              spark
                .sparkContext
                .parallelize(Seq(s"gene\t${sliceIndices.mkString("\t")}"))

            val rows =
              ds
                .slice(sliceIndices)
                .rdd
                .map(e => s"${e.gene}\t${e.values.toArray.map(_.toInt).mkString("\t")}" )

            (header union rows)
              .repartition(1)
              .saveAsTextFile(s"$outDir/${subSetFile.getName}.slice.$sliceIdx")

          }}

        } catch {
          case e: Throwable => e.printStackTrace
        }
      }
  }

}