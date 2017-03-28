package org.tmoerman.brassica.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader
import org.tmoerman.brassica.{RegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5EcoliBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8
  )

  val params =
    RegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  "the Dream5 e.coli pipeline" should "run" in {
    val (dataFile, tfFile) = network(3)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    // val targets = Seq("G10")

    val machine = PropsReader.currentProfile.get

    val suffix = params.hashCode()
    val out = s"${PropsReader.props("dream5Out")}e.coli_$suffix"

    deleteDirectory(new File(out))

    val result =
      ScenicPipeline
        .apply(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          // targets = targets.toSet, -> ALL
          params = params,
          nrPartitions = Some(spark.sparkContext.defaultParallelism))

    result.show

    result.repartition(1).write.csv(out)
  }

}