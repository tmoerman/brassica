package org.tmoerman.grnboost.cases.zeisel

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.cases.DataReader._
import org.tmoerman.grnboost.util.TimeUtils
import org.tmoerman.grnboost.util.TimeUtils.pretty

/**
  * @author Thomas Moerman
  */
object ZeiselInference {

  val boosterParams = Map(
    "seed" -> 777,
    "eta" -> 0.001,
    "subsample"         -> 0.25,
    "colsample_bytree"  -> 0.25,
    "gamma"             -> 10,
    "max_depth"         -> 1,
    "num_parallel_tree" -> 1,
    "silent" -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 1000,
      boosterParams = boosterParams)

  def main(args: Array[String]): Unit = {
    val in           = args(0)
    val mouseTFs     = args(1)
    val out          = args(2)
    val nrPartitions = args(3).toInt
    val nrThreads    = args(4).toInt

    val parsed =
      s"""
         |Args:
         |* in              = $in
         |* mouseTFs        = $mouseTFs
         |* output          = $out
         |* nr partitions   = $nrPartitions
         |* nr xgb threads  = $nrThreads
      """.stripMargin

    println(parsed)

    deleteDirectory(out)

    val (_, duration) = TimeUtils.profile {

      val spark =
        SparkSession
          .builder
          .appName(GRN_BOOST)
          .getOrCreate

      import spark.implicits._

      val ds  = readExpression(spark, in).cache
      val TFs = readTFs(mouseTFs).toSet

      val regulations =
        GRNBoost
          .inferRegulations(
            ds,
            candidateRegulators = TFs,
            params = params.copy(
              boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
            nrPartitions = Some(nrPartitions))
          .cache

      regulations
        .sumGainScores(params)
        .addElbowGroups(params)
        .sort($"regulator", $"target", $"gain".desc)
        .saveTxt(out)
    }

    println(s"\nGRNBoost wall time: ${pretty(duration)}\n")
  }

}