package org.aertslab.grnboost

import org.aertslab.grnboost.util.PropsReader.props
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class GRNBoostRunSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "dry-run" should "run" in {
    val args = Array(
      "infer",
      "-i", "src/test/resources/genie3/data.txt", "--transposed",
      "-tf", "src/test/resources/TF/mm9_TFs.txt",
      "-o", "src/test/resources/genie3/out.txt",
      "--dry-run")

    val inferenceCfg = CLI.parse(args).get.inf.get

    val (_, params) = GRNBoost.run(inferenceCfg)

    inferenceCfg.goal shouldBe DRY_RUN

    println(params)
  }

  val zeiselFiltered = props("zeiselFiltered")

  "cfg-run" should "pass smoke test" in {
    val args = Array(
      "infer",
      "-i", zeiselFiltered, "--skip-headers", "1",
      "-tf", "src/test/resources/TF/mm9_TFs.txt",
      "-o", "src/test/resources/zeisel/out.txt",
      "--nr-estimation-genes", "2",
      "--cfg-run")

    val inferenceCfg = CLI.parse(args).get.inf.get

    val (updatedCfg, params) = GRNBoost.run(inferenceCfg)

    updatedCfg.estimationSet.right.get.size shouldBe 2
  }

}