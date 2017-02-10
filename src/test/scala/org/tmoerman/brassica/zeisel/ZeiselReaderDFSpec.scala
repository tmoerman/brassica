package org.tmoerman.brassica.zeisel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.brassica.DataPaths._
import ZeiselReaderDF._

/**
  * @author Thomas Moerman
  */
class ZeiselReaderDFSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "DF reader"

  val mRNA = zeisel + "expression_mRNA_17-Aug-2014.txt"

  it should "parse the schema correctly" ignore  {
    val lines = rawLines(spark, mRNA)

    val schema = parseSchema(lines)

    schema.size shouldBe 19982

    schema.exists(_.name == "cell_id") shouldBe true
  }

  it should "parse the rows correctly" in {
    val lines = rawLines(spark, mRNA)

    val schema = parseSchema(lines)

    val rows = parseRows(lines, schema.size, maxCells = Some(5))

    println(rows.map(_.toSeq.take(20).mkString(", ")).collect.mkString("\n"))

    rows.count shouldBe 3005
  }

  it should "parse the DataFrame correctly" in {
    val df = apply(spark, mRNA, Some(20))

    df.createOrReplaceTempView("cells")

    spark.sql("SELECT * from cells").show(3)
  }

}