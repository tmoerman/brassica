package org.tmoerman.brassica.cases.megacell

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

/**
  * Save to Parquet, just to try out the Spark pipeline.
  *
  * @author Thomas Moerman
  */
class MegacellParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  it should "parse the data frame" in {
    if (! new File(megacellParquet).exists) {
      val (df, genes) = MegacellReader.apply(spark, megacell).get

      df.show(20, truncate = true)

      df.write.parquet(megacellParquet)
    }
  }

}