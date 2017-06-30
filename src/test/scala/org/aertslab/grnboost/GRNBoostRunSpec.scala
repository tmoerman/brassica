package org.aertslab.grnboost

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class GRNBoostRunSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "dry-run"

  it should "run" in {

    val args = Array(
      "infer",
      "-i", "src/test/resources/genie3/data.txt", "--transposed",
      "-tf", "src/test/resources/TF/mm9_TFs.txt",
      "-o", "src/test/resources/genie3/out.txt",
      "--dry-run")

    val inferenceCfg = CLI.parse(args).get.inf.get

    val params = GRNBoost.run(inferenceCfg)

    inferenceCfg.goal shouldBe DRY_RUN

    println(params)

    params.toString
  }

}