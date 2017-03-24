package org.tmoerman.brassica.cases.dream5

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{RegressionParams, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Dream5EcoliBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2
  )

  val params =
    RegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

}