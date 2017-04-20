package org.tmoerman.grnboost.cases.macosko

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.cases.DataReader._

/**
  * @author Thomas Moerman
  */
object MacoskoInference {

  val boosterParams = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 6,
    "max_depth" -> 6,
    "silent" -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams)

  def main(args: Array[String]): Unit = {
    val in           = args(0)
    val mouseTFs     = args(1)
    val out          = args(2)
    val nrPartitions = args(3).toInt
    val nrThreads    = args(4).toInt
    val nrRounds     = args(5).toInt

    val parsed =
      s"""
         |Args:
         |* in              = $in
         |* mouseTFs        = $mouseTFs
         |* output          = $out
         |* nr partitions   = $nrPartitions
         |* nr xgb threads  = $nrThreads
         |* nr xgb rounds   = $nrRounds
      """.stripMargin

    println(parsed)

    deleteDirectory(out)

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    val ds  = readExpression(spark, in).cache
    val TFs = readTFs(mouseTFs).map(_.toUpperCase).toSet // !!! uppercase for Macosko genes

    val regulations =
      GRNBoost
        .inferRegulations(
          ds,
          candidateRegulators = TFs,
          params = params.copy(
            nrRounds = nrRounds,
            boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
          nrPartitions = Some(nrPartitions))
        .cache

    regulations
      .normalize
      .saveTxt(out)
  }

}