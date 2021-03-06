package org.aertslab.grnboost.cases.macosko

import org.apache.spark.sql.SparkSession
import org.aertslab.grnboost._
import DataReader._
import org.aertslab.grnboost.util.TimeUtils._

/**
  * @author Thomas Moerman
  */
object MacoskoInference {

  val boosterParams = Map(
    "seed"              -> 777,
    "eta"               -> 0.01,
    "subsample"         -> 0.8,
    "colsample_bytree"  -> 0.25,
    "max_depth"         -> 1,
    "silent"            -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = Some(250),
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

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val profiles =
      Seq(250, 500).map { currentNrRounds =>

        print(s"Calculating GRN with $currentNrRounds boosting rounds...")

        val (_, duration) = profile {

          // reading input here to make timings honest
          val ds  = readExpressionsByGene(spark, in).cache
          val TFs = readRegulators(mouseTFs).map(_.toUpperCase).toSet // !!! uppercase for Macosko genes

          val currentParams =
            params.copy(
              nrRounds = Some(currentNrRounds),
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
            .sort($"regulator", $"target", $"gain".desc)
            .saveTxt(s"${out}stumps_${params.nrRounds}_rounds_eta_0.01")

        }

        println(s"Calculation with $currentNrRounds boosting rounds: ${pretty(duration)}")

        (currentNrRounds, duration)
      }

    profiles
      .foreach{ case (nrRounds, duration) =>
        println(s"Wall time with $nrRounds boosting rounds: ${pretty(duration)}")
      }
  }

}