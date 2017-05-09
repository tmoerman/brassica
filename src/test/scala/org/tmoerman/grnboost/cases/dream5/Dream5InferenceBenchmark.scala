package org.tmoerman.grnboost.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.util.PropsReader
import org.tmoerman.grnboost.{GRNBoostSuiteBase, XGBoostRegressionParams, _}

/**
  * @author Thomas Moerman
  */
class Dream5InferenceBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "max_depth" -> 5
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 7,
      boosterParams = boosterParams)

  "Dream5 regulation inference" should "run" in {
    Seq(1, 3, 4).foreach(n => computeNetwork(n, params))
  }

  private def computeNetwork(idx: Int, regressionParams: XGBoostRegressionParams): Unit = {
    println(s"computing network $idx")

    import spark.implicits._

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val path = s"${PropsReader.props("dream5Out")}/Shallow_boosters/Network${idx}norm/"

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