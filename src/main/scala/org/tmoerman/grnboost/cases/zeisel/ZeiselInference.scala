package org.tmoerman.grnboost.cases.zeisel

import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.cases.DataReader._
import org.tmoerman.grnboost.util.TimeUtils._

/**
  * @author Thomas Moerman
  */
object ZeiselInference {

  val boosterParams = Map(
    "seed" -> 777,
    "eta" -> 0.001,
    "subsample"         -> 0.8,  //
    "colsample_bytree"  -> 0.25, //
    "max_depth"         -> 1,    // stumps
    // "num_parallel_tree" -> 1,
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

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val ds  = readExpression(spark, in).cache
    val TFs = readTFs(mouseTFs).toSet

    val profiles =
      Seq(100, 250, 500, 1000, 2500, 5000).map { currentNrRounds =>

        print(s"Calculating GRN with $currentNrRounds boosting rounds...")

        val (_, duration) = profile {

          val currentParams =
            params.copy(
              nrRounds = currentNrRounds,
              boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads))

          val regulations =
            GRNBoost
              .inferRegulations(
                ds,
                candidateRegulators = TFs,
                params = currentParams,
                nrPartitions = Some(nrPartitions))
              .cache
          
          regulations
            //.sumGainScores(params)
            .addElbowGroups(params)
            .sort($"regulator", $"target", $"gain".desc)
            .saveTxt(s"${out}stumps_${currentNrRounds}_rounds")
        }

        println(s"Calculation with $currentNrRounds boosting rounds: ${pretty(duration)}")

        (currentNrRounds, duration)
      }

    profiles
      .foreach{ case (nrRounds, duration) =>
        println(s"Calculation with $nrRounds boosting rounds: ${pretty(duration)}")
      }
  }

}