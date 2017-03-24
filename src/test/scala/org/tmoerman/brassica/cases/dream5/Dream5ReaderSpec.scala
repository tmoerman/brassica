package org.tmoerman.brassica.cases.dream5

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.dream5.Dream5Reader._

/**
  * @author Thomas Moerman
  */
class Dream5ReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dream5Reader reading the original data"

  it should "parse the in silico data" in {
    val (dataFile, tfFile) = network(1)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    tfs.size            shouldBe 195
    ds.count            shouldBe 1643
    ds.head.values.size shouldBe 805
  }

  it should "parse the s. aureus data" in {
    val (dataFile, tfFile) = network(2)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    tfs.size            shouldBe 99
    ds.count            shouldBe 2810
    ds.head.values.size shouldBe 160
  }

  it should "parse the e. coli data" in {
    val (dataFile, tfFile) = network(3)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    ds.show()

    tfs.size            shouldBe 334
    ds.count            shouldBe 4511
    ds.head.values.size shouldBe 805
  }

  it should "parse the s. cerevisiae" in {
    val (dataFile, tfFile) = network(4)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    tfs.size            shouldBe 333
    ds.count            shouldBe 5950
    ds.head.values.size shouldBe 536
  }

}