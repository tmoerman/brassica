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

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8
  )

  val params =
    RegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams,
      nrFolds = 10)

  "Dream5 networks challenges" should "run" in {
    Seq(1, 2, 3, 4).foreach(computeNetwork)
  }

  private def computeNetwork(idx: Int): Unit = {
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
          nrPartitions = Some(spark.sparkContext.defaultParallelism))

    result
      .repartition(1)
      .rdd
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(out)
  }

}