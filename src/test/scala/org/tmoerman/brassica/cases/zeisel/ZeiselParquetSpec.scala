package org.tmoerman.brassica.cases.zeisel

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader._

/**
  * @author Thomas Moerman
  */
class ZeiselParquetSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Zeisel Parquet I/O"

  it should "write the gene expression DF to parquet" in {
    if (! new File(zeiselParquet).exists) {
      val (df, _) = apply(spark, zeiselMrna)

      df.write.parquet(zeiselParquet)
    }
  }

  it should "read the gene expression DF from .parquet" in {
    val (df, genes) = fromParquet(spark, zeiselParquet, zeiselMrna)

    df.show(5, truncate = true)

    df.count shouldBe 3005

    genes.size shouldBe 10
  }

}
