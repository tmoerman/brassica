package org.tmoerman.brassica.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.algo.InferXGBoostRegulations._
import org.tmoerman.brassica.util.PropsReader
import org.tmoerman.brassica.{XGBoostRegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5NetworksBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParamsSilico = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "min_child_weight" -> 6,
    "max_depth" -> 6
  )

  val paramsSilico =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParamsSilico)

  val boosterParamsBio = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 6,
    "max_depth" -> 6
  )

  val paramsBio =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParamsBio)

  "Dream5 regulation inference" should "run" in {
    Seq(
      (1, paramsSilico),
      (3, paramsBio),
      (4, paramsBio)).foreach(t => computeNetwork(t._1, t._2))
  }
  
  private def computeNetwork(idx: Int, regressionParams: XGBoostRegressionParams): Unit = {
    println(s"computing network $idx")

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val path = s"${PropsReader.props("dream5Out")}/Try2/Network${idx}norm_max/"

    val regulationDS =
      ScenicPipeline
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = regressionParams,
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache()

    deleteDirectory(new File(path))

    Some(regulationDS)
      .map(normalizeBySum)
      .map(keepTop())
      .foreach(saveTxt(path))
  }

}