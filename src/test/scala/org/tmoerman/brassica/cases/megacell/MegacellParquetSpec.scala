package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode.Append
import org.scalatest.{FlatSpec, Matchers}

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  it should "write the first 100 columns to Parquet" ignore {
    val top = 100

    val csc = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(0 until top)).get

    val genes = MegacellReader.readGeneNames(megacell).get.take(top)

    val df = MegacellReader.toColumnDataFrame(spark, csc, genes)

    df.write.parquet(megacellParquet)
  }

  it should "write the entire matrix to Parquet" in {
    val (nrCells, nrGenes) = MegacellReader.readDimensions(megacell).get

    val windowSize = 1000

    (0 until nrGenes)
      .sliding(windowSize, windowSize)
      .foreach { range =>
        println("reading csc matrix")
        val csc = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(range)).get

        println("reading genes")
        val genes = MegacellReader.readGeneNames(megacell).get.slice(range.head, windowSize)

        println("transforming csc matrix into a dataframe")
        val df = MegacellReader.toColumnDataFrame(spark, csc, genes)

        df.write.mode(Append).parquet(megacellParquet)
      }
  }

  it should "read the dataframe from Parquet" ignore {

    val df = spark.read.parquet(megacellParquet)

    df.show(20)
  }

}