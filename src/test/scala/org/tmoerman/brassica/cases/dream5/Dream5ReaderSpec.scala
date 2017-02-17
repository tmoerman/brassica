package org.tmoerman.brassica.cases.dream5

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class Dream5ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dream5Reader"

  def dream5wd = props("dream5")

  def dream5ecoli   = dream5(dream5wd + "Ecoli/",       "ecoli")
  def dream5saureus = dream5(dream5wd + "Saureus/",     "saureus")
  def dream5yeast   = dream5(dream5wd + "Scerevisiae/", "yeast")

  def dream5(dir: String, species: String) =
    Seq(
      s"${species}_data.tsv",
      s"${species}_gene_names.tsv",
      s"${species}_tf_names.tsv")
      .map(dir + _)

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