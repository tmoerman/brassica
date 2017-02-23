package org.tmoerman.brassica.cases.zeisel

import java.io.File

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

  def mouseTFs = props("mouseTFs")

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

  it should "parse the mouse TFs properly" in {
    val mm9_TFs = TFs(mouseTFs)

    mm9_TFs.size shouldBe 1623
  }

  val zeiselParquet = props("zeiselParquet")

  it should "write the gene expression DF to parquet" in {
    // only if it doesn't exist yet
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