package org.tmoerman.brassica.cases.dream5

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.dream5.Dream5Reader._

/**
  * @author Thomas Moerman
  */
class Dream5ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dream5Reader reading the original data"

  it should "parse ecoli" in {
    val (df, genes) = readOriginalData(spark, ecoliData, ecoliGenes)

    val tfs = readTFs(ecoliTFs)

    df.count shouldBe 805
    genes.length shouldBe 4297
    tfs.length shouldBe 304
  }

  it should "parse s. aureus" in {
    val (df, genes) = readOriginalData(spark, saureusData, saureusGenes)

    val TFs = readTFs(saureusTFs)

    df.count shouldBe 160
    genes.length shouldBe 2677
    TFs.length shouldBe 90
  }

  it should "parse s. cerevisiae" in {
    val (df, genes) = readOriginalData(spark, yeastData, yeastGenes)

    val TFs = Dream5Reader.readTFs(yeastTFs)

    df.count shouldBe 536 // experiments file only contains 535...?
    genes.length shouldBe 5667
    TFs.length shouldBe 183
  }

  behavior of "Dream5Reader reading the training data"

  it should "parse ecoli" in {
    // val (df, genes) = readTrainingData(spark, )
    fail("fixme")
  }

  it should "parse s. aureus" in {
    fail("fixme")
  }

  it should "parse s. cerevisiae" in {
    fail("fixme")
  }

}