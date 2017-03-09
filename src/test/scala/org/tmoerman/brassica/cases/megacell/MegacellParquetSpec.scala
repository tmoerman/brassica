package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SaveMode.Append
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.megacell.MegacellReader._

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Megacell to Parquet"

  it should "write the first 100 columns to Parquet" ignore {
    val top = 100

    val csc = readCSCMatrix(megacell, onlyGeneIndices = Some(0 until top)).get

    val genes = readGeneNames(megacell).get.take(top)

    val df = toColumnDataFrame(spark, csc, genes)

    df.write.parquet(megacellParquet)
  }
  
  it should "write the entire matrix to Parquet" in {
    val (nrCells, nrGenes) = readDimensions(megacell).get

    val genes = readGeneNames(megacell).get

    val windowSize = 1000

    (0 until nrGenes)
      .sliding(windowSize, windowSize)
      .take(3)
      .foreach { range =>
        println(s"reading csc matrix columns ${range.head} -> ${range.last}")

        val genesInRange = genes.slice(range.head, range.head + range.size)

        val csc = readCSCMatrix(megacell, onlyGeneIndices = Some(range), reindex = true).get

        val df = toColumnDataFrame(spark, csc, genesInRange)

        println
        df.show(5)
        df.write.mode(Append).parquet(megacellParquet)
        println
      }
  }

  it should "read the dataframe from Parquet" in {
    val df = spark.read.parquet(megacellParquet)

    df.show(20)
    println(df.count)
  }

}