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
      normalizeImportances = true,
      nrFolds = 10)

  "Dream5 networks challenges" should "run" in {
    // Seq(1, 3, 4).foreach(computeNetwork)

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
          expressionByGene.standardized,
          candidateRegulators = tfs.toSet,
          params = params,
          targets = Set("G10"),
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache

    result
      .sort($"importance".desc)
      .show()

    println(result.count)

//    sorted
//        .repartition(1)
//        .rdd
//        .map(r => {
//          val result = r.productIterator.mkString("\t")
//
//          println(r.regulator, r.target, result)
//
//          result
//        })
      //.saveAsTextFile(out)
  }

}