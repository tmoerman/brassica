package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{SaveMode, Dataset, DataFrame}
import org.apache.spark.sql.SaveMode.{Overwrite, Append}
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ExpressionByGene, GeneIndex, CellCount, Gene}
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

  it should "write the entire matrix to a Parquet Dataset of ExpressionByGene tuples" ignore {
    val (_, nrGenes) = readDimensions(megacell).get

    val globalGeneIndex = readGeneNames(megacell).get.zipWithIndex

    val parquetFile = megacellColumnsParquet

    val blockWidth = 1000

    (0 until nrGenes)
      .sliding(blockWidth, blockWidth)
      .foreach { range =>
        println(s"reading csc matrix columns ${range.head} -> ${range.last}")

        val subIndex = globalGeneIndex.slice(range.head, range.head + range.size)

        readBlockDS(subIndex).write.mode(Append).parquet(parquetFile)

        println
      }
  }

  it should "read the full parquet into a Dataset[ExpressionByGene]" in {
    import spark.implicits._

    val parquetFile = megacellColumnsParquet + "_full"

    val ds = spark.read.parquet(parquetFile).as[ExpressionByGene]

    ds.show(5)
  }

  it should "write the entire matrix to column vector Parquet" ignore {
    val (_, nrGenes) = readDimensions(megacell).get

    val genes = readGeneNames(megacell).get

    val parquetFile = megacellColumnsParquet

    val blockWidth = 1000

    (0 until nrGenes)
      .sliding(blockWidth, blockWidth)
      .foreach { range =>
        println(s"reading csc matrix columns ${range.head} -> ${range.last}")

        readBlockDF(range, genes).write.mode(Append).parquet(parquetFile)

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

        readBlockDF(range, genes, cellTop).write.mode(Append).parquet(parquetFile)

        println
      }
  }

  private def readBlockDS(genesIndex: List[(Gene, GeneIndex)],
                          cellTop: Option[CellCount] = None): Dataset[ExpressionByGene] = {

    val csc =
      MegacellReader
        .readCSCMatrix(
          megacell,
          cellTop = cellTop,
          onlyGeneIndices = Some(genesIndex.map(_._2)))
        .get

    MegacellReader.toExpressionByGeneDataset(spark, csc, genesIndex.map(_._1))
  }

  private def readBlockDF(geneIndexRange: Seq[Int], genes: List[Gene], cellTop: Option[CellCount] = None): DataFrame = {
    val genesInRange = genes.slice(geneIndexRange.head, geneIndexRange.head + geneIndexRange.size)

    val csc =
      MegacellReader
        .readCSCMatrix(
          megacell,
          cellTop = cellTop,
          onlyGeneIndices = Some(geneIndexRange))
        .get

    MegacellReader.toColumnDataFrame(spark, csc, genesInRange)
  }

  it should "read the dataframe from column vector Parquet" in {
    val df = spark.read.parquet(megacellColumnsParquet + "_10k")

    df.show(20)

    println(df.count)
  }

}