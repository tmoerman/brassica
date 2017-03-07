package org.tmoerman.brassica.cases.megacell

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  it should "write the first 100 columns to Parquet" in {
    val top = 100

    val csc = MegacellReader.readCSCMatrix(megacell, onlyGeneIndices = Some(0 until top)).get

    val genes = MegacellReader.readGeneNames(megacell).get.take(top)

    val df = MegacellReader.toColumnDataFrame(spark, csc, genes)

    df.write.parquet(megacellParquet)
  }

}