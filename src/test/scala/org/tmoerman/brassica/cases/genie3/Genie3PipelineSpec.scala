package org.tmoerman.brassica.cases.genie3

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{DataPaths, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Genie3PipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "ScenicPipeline"

  it should "run on the Genie3 test data" in {

    val TFs = List("CD19", "CDH17", "RAD51", "OSR2", "TBX3")

    val GRN =
      ScenicPipeline(
        spark,
        DataPaths.genie3,
        Genie3Reader,
        TFs,
        nrRounds = 10,
        nrWorkers = 4)

    GRN.show(20)
  }

}
