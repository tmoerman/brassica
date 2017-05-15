package org.tmoerman.grnboost.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.util.TimeUtils.pretty
import org.tmoerman.grnboost.util.{PropsReader, TimeUtils}
import org.tmoerman.grnboost.{GRNBoostSuiteBase, XGBoostRegressionParams, _}

/**
  * @author Thomas Moerman
  */
class Dream5InferenceBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "eta" -> 0.001,
    "subsample"         -> 0.8,  //
    "colsample_bytree"  -> 0.25, //
    "max_depth"         -> 1,    // stumps
    "silent" -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 1000,
      boosterParams = boosterParams)

  "Dream5 regulation inference" should "run" in {
    val (_, duration) = TimeUtils profile {
      Seq(1, 3, 4).foreach(n => computeNetwork(n, params))
    }

    println(s"\nGRNBoost wall time: ${pretty(duration)}\n")
  }

  private def computeNetwork(idx: Int, regressionParams: XGBoostRegressionParams): Unit = {
    println(s"computing network $idx")

    import spark.implicits._

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val path = s"${PropsReader.props("dream5Out")}/Stumps1000/Network${idx}norm/"

    deleteDirectory(new File(path))

    val regulations =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = regressionParams,
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache

    regulations
      .addElbowGroups(params)
      .sort($"regulator", $"target", $"gain".desc)
      .rdd
      .map(r => s"${r.regulator}\t${r.target}\t${r.gain}")
      .repartition(1)
      .saveAsTextFile(path)
  }

}