package org.tmoerman.grnboost.util

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest._
import org.tmoerman.grnboost.GRNBoostSuiteBase
import org.tmoerman.grnboost.cases.dream5.{Dream5Reader, network}
import org.tmoerman.grnboost.util.RankUtils.{saveSpearmanCorrelationMatrix, toRankings}

/**
  * @author Thomas Moerman
  */
class RankUtilsSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "toRankings"

  it should "work for empty list" in {
    val empty = List[Float]()

    toRankings(empty) shouldBe Nil
  }

  it should "work for a singleton" in {
    val single = List(1f)

    toRankings(single) shouldBe Seq(1)
  }

  it should "work for a small list" in {
    val list = List(0, 2, 3, 3, 0, 8, 8)

    toRankings(list) shouldBe Seq(2, 3, 4, 5, 1, 6, 7)
  }

  it should "work for a list of same things" in {
    val list = List(1, 1, 1, 1, 1)

    toRankings(list).toSet shouldBe (1 to 5).toSet
  }

  behavior of "saveCorrelationMatrix"

  it should "pass the smoke test on Dream data" in {
    val (dataFile, tfFile) = network(2)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val out = "src/test/resources/out/spearman"

    deleteDirectory(new File(out))

    saveSpearmanCorrelationMatrix(ds, tfs.toSet, out)
  }

}