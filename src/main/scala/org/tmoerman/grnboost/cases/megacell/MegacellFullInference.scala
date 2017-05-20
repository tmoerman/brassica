package org.tmoerman.grnboost.cases.megacell

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.joda.time.DateTime.now
import org.tmoerman.grnboost.cases.DataReader.readTFs
import org.tmoerman.grnboost.util.IOUtils.writeToFile
import org.tmoerman.grnboost.util.TimeUtils.{pretty, profile}
import org.tmoerman.grnboost.{ExpressionByGene, GRNBoost, GRN_BOOST, Regulation, XGBoostRegressionParams}

/**
  * @author Thomas Moerman
  */
object MegacellFullInference {

  val boosterParams = Map(
    "seed"              -> 777,
    "eta"               -> 0.01,
    "subsample"         -> 0.8,
    "colsample_bytree"  -> 0.25,
    "max_depth"         -> 1,
    "silent" -> 1
  )

  def main(args: Array[String]): Unit = {

    val parquet          = args(0)
    val mouseTFs         = args(1)
    val out              = args(2)
    val nrCellsPerPhase  = args(3).toInt
    val nrBoostingRounds = args(4).toInt
    val nrPartitions     = args(5).toInt
    val nrThreads        = args(6).toInt

    val parsedArgs =
      s"""
         |Args:
         |* parquet            = $parquet
         |* mouseTFs           = $mouseTFs
         |* output             = $out
         |* nr cells per phase = $nrCellsPerPhase
         |* nr boosting rounds = $nrBoostingRounds
         |* nr partitions      = $nrPartitions
         |* nr xgb threads     = $nrThreads
      """.stripMargin

    val outDir     = s"$out/full.stumps.$nrBoostingRounds.cells.per.phase.$nrCellsPerPhase.parts.${now}"
    val sampleFile = s"$out/full.stumps.$nrBoostingRounds.cells.per.phase.$nrCellsPerPhase.sample.txt"
    val infoFile   = s"$out/full.stumps.$nrBoostingRounds.cells.per.phase.$nrCellsPerPhase.info.txt"
    val timingFile = s"$out/full.stumps.$nrBoostingRounds.cells.per.phase.$nrCellsPerPhase.info.txt"

    println(parsedArgs)
    writeToFile(infoFile, parsedArgs + "\nbooster params:\n" + boosterParams.mkString("\n") + "\n")

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val (_, duration) = profile {

      val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache
      val TFs = readTFs(mouseTFs).toSet

      val totalCellCount = ds.head.values.size

      val phaseCellSets =
        (0 until totalCellCount)
          .sliding(nrCellsPerPhase, nrCellsPerPhase)
          .toList
          .reverse match {
            case x :: y :: rest => y ++ x :: rest
            case _              => ???
          }

      phaseCellSets
        .zipWithIndex
        .map{ case (phaseCellIndices, phaseIndex) => {

          println(s"Executing phase $phaseIndex")

          val phaseDS = ds.slice(phaseCellIndices)

          val params =
            new XGBoostRegressionParams(
              nrRounds = nrBoostingRounds,
              boosterParams = boosterParams)

          GRNBoost
            .inferRegulations(
              phaseDS,
              candidateRegulators = TFs,
              params = params,
              nrPartitions = Some(nrPartitions))
        }}
        .reduce(_ union _)
        .groupBy($"regulator", $"target")
        .agg(sum($"gain").as("gain"))
        .select($"regulator", $"target", $"gain")
        .as[Regulation]
        .saveTxt(outDir)

    }

    val timingInfo =
      s"""
         |* results written to: $outDir
         |* wall time: ${pretty(duration)}
       """.stripMargin

    println(timingInfo)
    writeToFile(timingFile, timingInfo)
  }


}