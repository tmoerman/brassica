package org.tmoerman.grnboost.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.{GRNBoostSuiteBase, randomSubset}

/**
  * @author Thomas Moerman
  */
class MegacellSample100K extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "it" should "work" in {
    val subset = randomSubset(nr, 0 until totalCellCount)


  }

}