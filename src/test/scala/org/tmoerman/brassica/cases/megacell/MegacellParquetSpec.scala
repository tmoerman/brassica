package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode.Append
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.TimeUtils

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

  it should "write the entire matrix to Parquet" ignore {
    val (nrCells, nrGenes) = MegacellReader.readDimensions(megacell).get

    val windowSize = 1000

    val df =
      (0 until nrGenes)
        .sliding(windowSize, windowSize)
        .map { range =>

          println(s"reading csc matrix columns ${range.head} -> ${range.last}")

          val (df, duration) = TimeUtils.profile {

            println
            val csc = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(range)).get
            println

            val genes = MegacellReader.readGeneNames(megacell).get.slice(range.head, range.size)

            val df = MegacellReader.toColumnDataFrame(spark, csc, genes)

            df
          }

          println(s"reading csc matrix columns ${range.head} -> ${range.last} took ${duration.toMinutes} minutes")

          df }
        .reduce(_ union _)

    println("appending dataframe to parquet file")
    df.write.parquet(megacellParquet)

    println
  }

  it should "read the dataframe from Parquet" in {

    val df = spark.read.parquet(megacellParquet)

    df.show(20)

    println(df.count)
  }

}