package org.tmoerman.grnboost.cases.megacell

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime.now
import org.tmoerman.grnboost.cases.DataReader.readTFs
import org.tmoerman.grnboost.util.IOUtils.writeToFile
import org.tmoerman.grnboost.util.TimeUtils.{pretty, profile}
import org.tmoerman.grnboost._

import scala.io.Source
import scala.util.Try

/**
  * @author Thomas Moerman
  */
object MegacellInferenceFromSubSet {

  val boosterParams = Map(
    "seed"              -> 777,
    "eta"               -> 0.01,
    "subsample"         -> 0.8,
    "colsample_bytree"  -> 0.25,
    "max_depth"         -> 1,
    "silent" -> 1
  )

  val nrRounds = 100

  val params =
    XGBoostRegressionParams(
      nrRounds = nrRounds,
      boosterParams = boosterParams)

  def main(args: Array[String]): Unit = {

    val parquet        = args(0)
    val mouseTFs       = args(1)
    val out            = args(2)
    val cellSubSetFile = args(3)
    val cellSubSetID   = Try(args(4)).getOrElse("unknown")
    val nrPartitions   = args(5).toInt
    val nrThreads      = args(6).toInt

    val parsedArgs =
      s"""
         |Args:
         |* parquet          = $parquet
         |* mouseTFs         = $mouseTFs
         |* output           = $out
         |* cell subset file = $cellSubSetFile
         |* nr partitions    = $nrPartitions
         |* nr xgb threads   = $nrThreads
      """.stripMargin

    val outDir     = s"$out/stumps.$nrRounds.cells.from.subset.$cellSubSetID.${now}"
    val infoFile   = s"$out/stumps.$nrRounds.cells.from.subset.$cellSubSetID.targets.param.info.txt"
    val timingFile = s"$out/stumps.$nrRounds.cells.from.subset.$cellSubSetID.targets.timing.info.txt"

    println(parsedArgs)
    writeToFile(infoFile, parsedArgs + "\nbooster params:\n" + boosterParams.mkString("\n"))

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val (_, duration) = profile {

      val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache
      val TFs = readTFs(mouseTFs).toSet

      val cellIndicesSubSet: Seq[CellIndex] =
        Source.fromFile(cellSubSetFile).getLines.filterNot(_.isEmpty).map(_.trim.toInt).toSeq

      val slicedByCells = ds.slice(cellIndicesSubSet)

      val regulations =
        GRNBoost
          .inferRegulations(
            slicedByCells,
            candidateRegulators = TFs,
            params = params.copy(
              boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
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