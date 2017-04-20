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
      nrRounds = 12,
      boosterParams = boosterParams)

  def main(args: Array[String]): Unit = {
    val in        = args(0)
    val mouseTFs  = args(1)
    val out       = args(2)
    val nrCells   = if (args(3).toLowerCase == "all") None else Some(args(3).toInt)
    val nrTargets = if (args(4).toLowerCase == "all") None else Some(args(4).toInt)
    val nrPartitions = args(5).toInt
    val nrThreads    = args(6).toInt
    val nrRounds     = args(7).toInt

    val parsed =
      s"""
         |Args:
         |* in              = $in
         |* mouseTFs        = $mouseTFs
         |* output          = $out
         |* nr cells        = $nrCells
         |* nr target genes = $nrTargets
         |* nr xgb threads  = $nrThreads
      """.stripMargin

    println(parsed)

    val outDir = s"$out/run_${nrCells.getOrElse("ALL")}cells_${nrTargets.getOrElse("ALL")}targets"

    deleteDirectory(outDir)

    val spark =
      SparkSession
        .builder
        .master("local[*]")
        .appName(GRN_BOOST)
        .getOrCreate()

    val ds  = readExpression(spark, in).cache
    val TFs = readTFs(mouseTFs).map(_.toUpperCase).toSet

    val totalCellCount = ds.head.values.size

    val slicedByCells =
      nrCells
        .map(nr => {
          val subset = randomSubset(nr, 0 until totalCellCount)
          ds.slice(subset).cache
        })
        .getOrElse(ds)

    val targetSet =
      nrTargets
        .map(nr => slicedByCells.take(nr).map(_.gene).toSet)
        .getOrElse(Set.empty)

    val regulations =
      GRNBoost
        .inferRegulations(
          slicedByCells,
          candidateRegulators = TFs,
          params = params.copy(nrRounds = nrRounds, boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
          targets = targetSet,
          nrPartitions = Some(nrPartitions))
        .cache

    regulations
      .normalize
      .saveTxt(outDir)
  }

}