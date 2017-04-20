package org.tmoerman.grnboost.cases.dream5

import org.scalatest.FlatSpec

/**
  * @author Thomas Moerman
  */
class Dream5ScoreSpec extends FlatSpec {

  "Scoring GENIE3 (WOTC) on networks 1, 3, 4" should "work" in {
    val p_ROC = Seq("1.596591e-104", "5.149347e-20", "1.581591e-01").map(_.toDouble)
    val p_PR  = Seq("3.060302e-106", "5.003620e-11", "1.064168e-02").map(_.toDouble)

    val score34 = score(p_ROC.drop(1), p_PR.drop(1))
    val score134 = score(p_ROC, p_PR)

    println(s"GENIE3 score on networks: (3, 4) -> $score34")
    println(s"GENIE3 score on networks: (1, 3, 4) -> $score134")
  }

  "Scoring XGB on networks" should "work" in {
    val p_PR  = Seq("9.982996e-96", "5.687217e-24", "1.763807e-02").map(_.toDouble)
    val p_ROC = Seq("4.444600e-33", "2.297304e-08", "1.040680e-02").map(_.toDouble)

    val score34 = score(p_ROC.drop(1), p_PR.drop(1))
    val score134 = score(p_ROC, p_PR)

    println(s"XGB score on networks: (3, 4) -> $score34")
    println(s"XGB score on networks: (1, 3, 4) -> $score134")
  }

}