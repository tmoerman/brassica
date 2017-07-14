package org.aertslab.grnboost.cases.dream5

import java.io.File

import org.aertslab.grnboost.util.PropsReader
import org.aertslab.grnboost.util.TimeUtils.{pretty, profile}
import org.aertslab.grnboost.{GRNBoostSuiteBase, XGBoostRegressionParams, _}
import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class Dream5InferenceBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

//  val boosterParams = Map(
//    "seed"      -> 777,
//    "eta"       -> 0.1,
//    "max_depth" -> 2,
//    "silent"    -> 1
//  )

  val boosterParams = Map(
    "seed"      -> 777,
    "eta"       -> 0.001,
    "max_depth" -> 1,
    "colsample_bytree" -> 0.072,
    "silent"    -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 670,
      boosterParams = boosterParams)

  "Dream5 regulation inference" should "run" in {
    Seq(1, 3, 4)
      .map(n => computeNetwork(n, params))
      .foreach { case (nw, duration) => println(s"\n Dream5 nw$nw wall time: ${pretty(duration)}\n") }
  }

  private def computeNetwork(nw: Int, regressionParams: XGBoostRegressionParams) = {
    println(s"computing network $nw")

    import spark.implicits._

    val (_, duration) = profile {
      val (dataFile, tfFile) = network(nw)

      val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

      val path = s"${PropsReader.props("dream5Out")}/Stumps1000/Network${nw}norm/"

      deleteDirectory(new File(path))

      val regulations =
        GRNBoost
          .inferRegulationsIterated(
            expressionByGene,
            candidateRegulators = tfs.toSet,
            params = regressionParams,
            nrPartitions = Some(spark.sparkContext.defaultParallelism))
          .cache

      withRegularizationLabels(regulations, params)
        .sort($"regulator", $"target", $"gain".desc)
        .rdd
        .map(r => s"${r.regulator}\t${r.target}\t${r.gain}")
        .repartition(1)
        .saveAsTextFile(path)
    }

    (nw, duration)
  }

}