package org.tmoerman.brassica.cases.zeisel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.linalg.SparseVector
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader._
import org.tmoerman.brassica.EXPRESSION_VECTOR

/**
  * @author Thomas Moerman
  */
class ZeiselReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "ZeiselReader"

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

    df.count shouldBe ZEISEL_CELL_COUNT

    df.show(5)
  }

  it should "parse the mouse TFs properly" in {
    val mm9_TFs = readTFs(mouseTFs)

    mm9_TFs.size shouldBe 1623
  }

  it should "read column vectors correctly" in {
    val lines = ZeiselReader.rawLines(spark, zeiselMrna)

    val columnsDF = ZeiselReader.readColumnVectors(spark, lines)

    columnsDF.show(5)

    columnsDF.head.getAs[SparseVector](EXPRESSION_VECTOR).size shouldBe ZEISEL_CELL_COUNT

    columnsDF.count shouldBe ZEISEL_GENE_COUNT
  }


}