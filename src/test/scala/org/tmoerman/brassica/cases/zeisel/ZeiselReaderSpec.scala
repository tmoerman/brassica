package org.tmoerman.brassica.cases.zeisel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.DataPaths._
import ZeiselReader._

/**
  * @author Thomas Moerman
  */
class ZeiselReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "DF reader"

  val mRNA = zeisel + "expression_mRNA_17-Aug-2014.txt"

  it should "parse the schema correctly" in {
    val lines = rawLines(spark, mRNA)

    val schema = parseSchema(lines)

    schema.size shouldBe 11

    schema.exists(_.name == "cell_id") shouldBe true

    schema.exists(_.name == "expression") shouldBe true
  }

  it should "parse the DataFrame correctly" in {
    val df = apply(spark, mRNA)

    df.count shouldBe 3005
  }

}