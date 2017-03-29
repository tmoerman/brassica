package org.tmoerman.brassica.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader
import org.tmoerman.brassica.{RegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5NetworksBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParamsBAK = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.1,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    //"colsample_bylevel" -> 0.5,
    "min_child_weight" -> 3,
    "max_depth" -> 4,
    "gamma" -> 0
  )

  val boosterParamsEVO = Map(
    //"seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.1,
    //"subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    //"colsample_bylevel" -> 0.8,
    "min_child_weight" -> 3,
    "max_depth" -> 3,
    "gamma" -> 0
  )

  val boosterParams = Map(
    //"seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8
    //"colsample_bylevel" -> 0.5,
    //"min_child_weight" -> 3,
    //"max_depth" -> 4,
    //"gamma" -> 1
  )

  val params =
    RegressionParams(
      nrRounds = 125,
      boosterParams = boosterParamsBAK,
      nrFolds = 10)

  "Dream5 networks challenges" should "run" in {
    //Seq(1, 3, 4).foreach(computeNetwork)
    Seq(3).foreach(computeNetwork)
  }

  private def computeNetwork(idx: Int): Unit = {
    import spark.implicits._

    println(s"computing network $idx")

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val out = s"${PropsReader.props("dream5Out")}Network$idx/"
    deleteDirectory(new File(out))

    val result =
      ScenicPipeline
        .apply(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = params,
          targets = Set("G10"),
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache()

    result
      .sort($"importance".desc)
      .show(100)

//    result
//      .sort($"importance".desc)
//      .rdd
//      .zipWithIndex.filter(_._2 < 100000).keys // top 100K
//      .repartition(1)
//      .map(_.productIterator.mkString("\t"))
//      .saveAsTextFile(out)
  }

}