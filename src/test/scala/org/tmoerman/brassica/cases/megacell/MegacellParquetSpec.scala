package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.TimeUtils.profile

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Megacell to parquet"

  it should "write the first 100 columns to Parquet" in {
    val top = 100

    val (csc, _) = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(0 until top)).get

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

          println(s"reading csc matrix columns ${range.head} -> ${range.last} \n")

          val (df, duration) = profile {
            val (csc, _) = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(range)).get

            val genes = MegacellReader.readGeneNames(megacell).get.slice(range.head, range.size)

            val df = MegacellReader.toColumnDataFrame(spark, csc, genes)

            df
          }

          println(s"\nreading csc matrix columns ${range.head} -> ${range.last} took ${duration.toMinutes} minutes")

          df }
        .reduce(_ union _)

    println("writing dataframe to parquet file")
    df.write.parquet(megacellParquet)

    df.count shouldBe nrCells
  }

  it should "read the dataframe from Parquet" in {
    val df = spark.read.parquet(megacellParquet)

    df.show(20)
    println(df.count)
  }

}