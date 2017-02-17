package org.tmoerman.brassica.cases.genie3

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{DataPaths, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Genie3PipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "ScenicPipeline"

  it should "run on the Genie3 test data" in {

    val regulators = List("CD19", "CDH17", "RAD51", "OSR2", "TBX3")

    val (expression, genes) = Genie3Reader.apply(spark, DataPaths.genie3)

    val (grn, timings) =
      ScenicPipeline(
        spark,
        expression, genes, regulators,
        nrRounds = 10,
        nrWorkers = 4)

    grn.show(20)

    println(timings.mkString("\n"))
  }

}
