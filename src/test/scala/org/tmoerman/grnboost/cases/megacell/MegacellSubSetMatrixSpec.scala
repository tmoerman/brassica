package org.tmoerman.grnboost.cases.megacell

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
class MegacellSubSetMatrixSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "it" should "work" in {
    val subSetDir = "/media/tmo/data/work/datasets/megacell/out/cell.subsets/100k/nr.rounds.100"
    val outDir    = "/media/tmo/data/work/datasets/megacell/out/matrix.subsets/100k/nr.rounds.100"
    val parquet   = "/media/tmo/data/work/datasets/megacell/parquet_full"

    doit(parquet, subSetDir, outDir)
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
                .map(e => s"${e.gene}\t${e.values.toArray.mkString("\t")}" )

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