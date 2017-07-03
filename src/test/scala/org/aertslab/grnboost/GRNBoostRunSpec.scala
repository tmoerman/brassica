package org.aertslab.grnboost

import java.io.File

import org.aertslab.grnboost.util.PropsReader.props
import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * @author Thomas Moerman
  */
class GRNBoostRunSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "dry-run" should "run" in {
    val args = Array(
      "infer",
      "-i", "src/test/resources/genie3/data.txt", "--transposed",
      "-tf", "src/test/resources/TF/mm9_TFs.txt",
      "-o", "src/test/resources/genie3/out.meh",
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
      "-o", "src/test/resources/zeisel/out.meh",
      "--nr-estimation-genes", "2",
      "--cfg-run")

    val inferenceCfg = CLI.parse(args).get.inf.get

    val (updatedCfg, params) = GRNBoost.run(inferenceCfg)

    updatedCfg.estimationSet.right.get.size shouldBe 2
  }

  "run" should "pass smoke for 1 target" in {
    val outPath = "src/test/resources/zeisel/out.regularized"
    val outDir  = new File(outPath)

    val args = Array(
      "infer",
      "-i", zeiselFiltered, "--skip-headers", "1",
      "-tf", "src/test/resources/TF/mm9_TFs.txt",
      "-o", outPath,
      "--nr-estimation-genes", "2",
      "--targets", "Gad1")

    val inferenceCfg = CLI.parse(args).get.inf.get

    val (updatedCfg, params) = GRNBoost.run(inferenceCfg)

    updatedCfg.estimationSet.right.get.size shouldBe 2

    Source.fromFile(outPath + "/part-00000").getLines.size shouldBe 7

    deleteDirectory(outDir)
  }

}