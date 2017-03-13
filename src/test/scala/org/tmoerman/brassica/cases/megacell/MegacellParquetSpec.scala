package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SaveMode.Append
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{CellCount, Gene}
import org.tmoerman.brassica.cases.megacell.MegacellReader._

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Megacell to Parquet"

  it should "write the first 100 columns to column vector Parquet" ignore {
    val top = 100

    val csc = readCSCMatrix(megacell, onlyGeneIndices = Some(0 until top)).get

    val genes = readGeneNames(megacell).get.take(top)

    val df = toColumnDataFrame(spark, csc, genes)

    df.write.parquet(megacellColumnsParquet)
  }

  it should "write the entire matrix to column vector Parquet" ignore {
    val (_, nrGenes) = readDimensions(megacell).get

    val genes = readGeneNames(megacell).get

    val cellTop = None

    val parquetFile = megacellColumnsParquet

    val blockWidth = 1000

    (0 until nrGenes)
      .sliding(blockWidth, blockWidth)
      .foreach { range =>
        println(s"reading csc matrix columns ${range.head} -> ${range.last}")

        readBlock(range, genes, cellTop).write.mode(Append).parquet(parquetFile)

        println
      }
  }

  it should "write a top 10K cells matrix to column vector Parquet" in {
    val (_, nrGenes) = readDimensions(megacell).get

    val genes = readGeneNames(megacell).get

    val blockWidth = 1000

    val cellTop = Some(10000)

    val parquetFile = megacellColumnsParquet + "_10k"

    (0 until nrGenes)
      .sliding(blockWidth, blockWidth)
      .foreach { range =>
        println(s"reading csc matrix columns ${range.head} -> ${range.last}")

        readBlock(range, genes, cellTop).write.mode(Append).parquet(parquetFile)

        println
      }
  }

  private def readBlock(range: Seq[Int], genes: List[Gene], cellTop: Option[CellCount] = None) = {
    val genesInRange = genes.slice(range.head, range.head + range.size)

    val csc =
      readCSCMatrix(
        megacell,
        cellTop = cellTop,
        onlyGeneIndices = Some(range),
        reindex = true)
        .get

    toColumnDataFrame(spark, csc, genesInRange)
  }

  it should "read the dataframe from column vector Parquet" in {
    val df = spark.read.parquet(megacellColumnsParquet + "_10k")

    df.show(20)

    println(df.count)
  }

}