package org.tmoerman.brassica.cases.genie3

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{ScenicPipelineOld, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Genie3PipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "ScenicPipeline"

  it should "run on the Genie3 test data" in {

    val regulators = Set("CD19", "CDH17", "RAD51", "OSR2", "TBX3")

    val (expression, genes) = Genie3Reader.apply(spark, props("genie3"))

    val (grn, stats) =
      ScenicPipelineOld(
        spark,
        expression,
        genes,
        nrRounds = 10,
        regulators)

    grn.show(20)

    println(stats.mkString("\n"))

    // val outPath = out("genie3") TODO

    // grn.write.parquet(outPa) FIXME
  }

}
