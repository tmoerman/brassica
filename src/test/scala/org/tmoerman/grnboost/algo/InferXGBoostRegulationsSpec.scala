package org.tmoerman.grnboost.algo

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.algo.InferXGBoostRegulations._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
class InferXGBoostRegulationsSpec extends FlatSpec with Matchers {

  behavior of "parsing metrics"

  val treeDumpWithStats = Source.fromFile(new File("src/test/resources/xgb/treeDumpWithStats.txt")).getLines.mkString("\n")

  it should "parse tree metrics" in {
    val treeMetrics = parseTreeMetrics(treeDumpWithStats)

    treeMetrics.size shouldBe 28

    val (featureIdx, metrics) = treeMetrics(0)
    val (freq, gain, cover) = metrics

    featureIdx shouldBe 223

    freq  shouldBe 1
    gain  shouldBe 1012.38f
    cover shouldBe 2394f
  }

  behavior of "aggregating booster metrics"

  it should "aggregate correctly for 1 tree" in {
    val metrics = aggregateBoosterMetrics(Seq(treeDumpWithStats))

    val (f, g, c) = metrics(223)

    f shouldBe 2
    g shouldBe 1012.38f + 53.1558f
    c shouldBe 2394 + 1886
  }

  it should "aggregate correctly for multiple trees" in {
    val metrics = aggregateBoosterMetrics(Seq(treeDumpWithStats, treeDumpWithStats))

    val (f, g, c) = metrics(223)

    f shouldBe 2 * (2)
    g shouldBe 2 * (1012.38f + 53.1558f)
    c shouldBe 2 * (2394 + 1886)
  }

  behavior of "turning a list of elbow indices into a stream of elbow indicators"

  it should "work for empty elbow list" in {
    (0 until 5)
      .zip(toElbowStream(Nil))
      .toList shouldBe List.tabulate(5)(i => (i, None))
  }

  it should "work for 1 elbow" in {
    (0 until 5)
      .zip(toElbowStream(2 :: Nil))
      .toList shouldBe
      List(
        (0, Some(0)),
        (1, Some(0)),
        (2, Some(0)),
        (3, None),
        (4, None))
  }

  it should "work for 2 elbows" in {
    (0 until 5)
      .zip(toElbowStream(1 :: 3 :: Nil))
      .toList shouldBe
      List(
        (0, Some(0)),
        (1, Some(0)),
        (2, Some(1)),
        (3, Some(1)),
        (4, None))
  }

}