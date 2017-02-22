package org.tmoerman.brassica.cases.zeisel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader._
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "ZeiselReader"

  def zeiselMrna = props("zeisel")

  it should "parse the schema correctly" in {
    val lines = rawLines(spark, zeiselMrna)

    val schema = parseSchema(lines)

    schema.size shouldBe 11

    schema.exists(_.name == "cell_id") shouldBe true

    schema.exists(_.name == "expression") shouldBe true
  }

  it should "parse the gene names correctly" in {
    val lines = rawLines(spark, zeiselMrna)

    val genes = parseGenes(lines)

    genes.take(5) shouldBe List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")
  }

  it should "parse the DataFrame correctly" in {
    val (df, _) = apply(spark, zeiselMrna)

    df.count shouldBe 3005

    df.show(5, truncate = true)
  }

}