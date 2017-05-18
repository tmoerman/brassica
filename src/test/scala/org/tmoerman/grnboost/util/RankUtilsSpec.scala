package org.tmoerman.grnboost.util

import org.scalatest._
import org.tmoerman.grnboost.util.RankUtils.toRankings

/**
  * @author Thomas Moerman
  */
class RankUtilsSpec extends FlatSpec with Matchers {

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

}