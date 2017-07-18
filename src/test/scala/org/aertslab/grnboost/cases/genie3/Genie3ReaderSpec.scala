package org.aertslab.grnboost.cases.genie3

import org.aertslab.grnboost._
import org.aertslab.grnboost.util.PropsReader.props
import org.scalatest.tagobjects.Slow
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class Genie3ReaderSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "GenieReader"

  it should "parse the DataFrame correctly" taggedAs Slow in {
    val (df, genes) = Genie3Reader.apply(spark, props("genie3"))

    genes.size shouldBe 10

    df.columns shouldBe Array(EXPRESSION)

    df.count shouldBe 136

    df.show(3)
  }

}