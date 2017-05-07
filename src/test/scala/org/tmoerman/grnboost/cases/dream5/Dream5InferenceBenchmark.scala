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

    val path = s"${PropsReader.props("dream5Out")}/Try2/Network${idx}norm/"

    deleteDirectory(new File(path))

    val regulations =
      GRNBoost
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = regressionParams,
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache()

    import org.apache.spark.sql.functions._

//    FIXME incompatible with RawRegulations
//    regulations
//      .normalize(paramsBio)
//      .truncate()
//      .saveTxt(path)
  }

}