package org.aertslab.grnboost.cases.genie3

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost._
import org.aertslab.grnboost.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class Genie3ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "GenieReader"

  it should "parse the DataFrame correctly" in {
    val (df, genes) = Genie3Reader.apply(spark, props("genie3"))

    genes.size shouldBe 10

    df.columns shouldBe Array(EXPRESSION)

    df.count shouldBe 136

    df.show(3)
  }

}