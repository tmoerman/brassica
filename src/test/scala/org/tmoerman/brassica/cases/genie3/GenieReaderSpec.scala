package org.tmoerman.brassica.cases.genie3

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.brassica.DataPaths

import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
class GenieReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "GenieReader"

  it should "parse the DataFrame correctly" in {
    val (df, genes) = Genie3Reader.apply(spark, DataPaths.genie3)

    genes.size shouldBe 10

    df.columns shouldBe Array(EXPRESSION_VECTOR)

    df.count shouldBe 136

    df.show(3)
  }

}