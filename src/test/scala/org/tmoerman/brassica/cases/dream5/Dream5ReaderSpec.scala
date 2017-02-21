package org.tmoerman.brassica.cases.dream5

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class Dream5ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dream5Reader"

  it should "parse the ecoli data correctly" in {
    val (df, genes) = Dream5Reader(spark, ecoliData, ecoliGenes)

    val tfs = Dream5Reader.TFs(ecoliTFs)

    df.count shouldBe 805
    genes.length shouldBe 4297
    tfs.length shouldBe 304
  }

  it should "parse the s. aureus data correctly" in {
    val (df, genes) = Dream5Reader(spark, saureusData, saureusGenes)

    val TFs = Dream5Reader.TFs(saureusTFs)

    df.count shouldBe 160
    genes.length shouldBe 2677
    TFs.length shouldBe 90
  }

  it should "parse the s. cerevisiae data correctly" in {
    val (df, genes) = Dream5Reader(spark, yeastData, yeastGenes)

    val TFs = Dream5Reader.TFs(yeastTFs)

    df.count shouldBe 536 // experiments file only contains 535...?
    genes.length shouldBe 5667
    TFs.length shouldBe 183
  }

}