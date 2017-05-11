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
    // "seed" -> 777,
    "eta" -> 0.15,
    // "subsample" -> 0.5,
    "max_depth" -> 5,
    "num_parallel_tree" -> 3
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 7,
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

    val path = s"${PropsReader.props("dream5Out")}/Try/Network${idx}norm/"

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
      .select($"regulator", $"target", $"gain".as("importance"))
      .as[Regulation]
      .truncate()
      .saveTxt(path)
  }

}