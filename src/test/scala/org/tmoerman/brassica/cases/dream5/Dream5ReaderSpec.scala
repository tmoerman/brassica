package org.tmoerman.brassica.cases.dream5

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.brassica.DataPaths._

/**
  * @author Thomas Moerman
  */
class Dream5ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dream5Reader"

  it should "parse the ecoli data correctly" in {
    val (df, genes) = Dream5Reader(spark, dream5ecoli: _*)

    df.count shouldBe 805
    genes.length shouldBe 4297
  }

  it should "parse the s. aureus data correctly" in {
    val (df, genes) = Dream5Reader(spark, dream5saureus: _*)

    df.count shouldBe 160
    genes.length shouldBe 2677
  }

  it should "parse the s. cerevisiae data correctly" in {
    val (df, genes) = Dream5Reader(spark, dream5yeast: _*)

    df.count shouldBe 536 // experiments file only contains 535...?
    genes.length shouldBe 5667
  }

}