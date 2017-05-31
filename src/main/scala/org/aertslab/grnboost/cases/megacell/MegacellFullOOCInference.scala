package org.aertslab.grnboost.cases.megacell

import org.aertslab.grnboost.cases.DataReader.readTFs
import org.aertslab.grnboost.util.IOUtils.writeToFile
import org.aertslab.grnboost.util.TimeUtils.{pretty, profile}
import org.aertslab.grnboost.{ExpressionByGene, GRNBoost, GRN_BOOST, XGB_THREADS, XGBoostRegressionParams}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime.now

/**
  * @author Thomas Moerman
  */
object MegacellFullOOCInference {

  val boosterParams = Map(
    "seed"             -> 777,
    "eta"              -> 0.1,
    "subsample"        -> 0.8,
    "colsample_bytree" -> 0.25,
    "max_depth" -> 1,
    "silent"    -> 1
  )

  def main(args: Array[String]): Unit = {

    val parquet        = args(0)
    val mouseTFs       = args(1)
    val out            = args(2)
    val nrRounds       = args(3).toInt
    val nrPartitions   = args(4).toInt
    val nrThreads      = args(5).toInt

    val parsedArgs =
      s"""
         |Args:
         |* parquet            = $parquet
         |* mouseTFs           = $mouseTFs
         |* output             = $out
         |* nr boosting rounds = $nrRounds
         |* nr partitions      = $nrPartitions
         |* nr xgb threads     = $nrThreads
      """.stripMargin

    val outDir     = s"$out/stumps.$nrRounds.full.ooc.${now}"
    val infoFile   = s"$out/stumps.$nrRounds.full.ooc.params.txt"
    val timingFile = s"$out/stumps.$nrRounds.full.ooc.timing.txt"

    println(parsedArgs)
    writeToFile(infoFile, parsedArgs + "\nbooster params:\n" + boosterParams.mkString("\n") + "\n")

    val params =
      XGBoostRegressionParams(
        nrRounds = nrRounds,
        boosterParams = boosterParams + (XGB_THREADS -> nrThreads))

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val (_, duration) = profile {

      val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache
      val TFs = readTFs(mouseTFs).toSet

      val regulations =
        GRNBoost
          .inferRegulationsOutOfCore(
            expressionsByGene = ds,
            candidateRegulators = TFs,
            params = params,
            nrPartitions = Some(nrPartitions))
          .cache

      regulations
        .sort($"regulator", $"target", $"gain".desc)
        .rdd
        .map(r => s"${r.regulator}\t${r.target}\t${r.gain}")
        .repartition(1)
        .saveAsTextFile(outDir)
    }

    val timingInfo =
      s"results written to $outDir\n" +
        s"Wall time with ${params.nrRounds} boosting rounds: ${pretty(duration)}"

    println(timingInfo)
    writeToFile(timingFile, timingInfo)
  }

}