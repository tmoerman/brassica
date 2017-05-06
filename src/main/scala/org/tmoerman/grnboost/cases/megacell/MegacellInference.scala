package org.tmoerman.grnboost.cases.megacell

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.cases.DataReader._
import org.tmoerman.grnboost.util.IOUtils.writeToFile

/**
  * @author Thomas Moerman
  */
object MegacellInference {

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
    // FIXME better arg parsing
    val parquet      = args(0)
    val mouseTFs     = args(1)
    val out          = args(2)
    val nrCells      = if (args(3).toLowerCase == "all") None else Some(args(3).toInt)
    val nrTargets    = if (args(4).toLowerCase == "all") None else Some(args(4).toInt)
    val nrPartitions = args(5).toInt
    val nrThreads    = args(6).toInt
    val nrRounds     = args(7).toInt

    val parsedArgs =
      s"""
        |Args:
        |* parquet         = $parquet
        |* mouseTFs        = $mouseTFs
        |* output          = $out
        |* nr cells        = $nrCells
        |* nr target genes = $nrTargets
        |* nr partitions   = $nrPartitions
        |* nr xgb threads  = $nrThreads
        |* nr xgb rounds   = $nrRounds
      """.stripMargin

    val outDir     = s"$out/megacell_${nrCells.getOrElse("ALL")}cells_${nrTargets.getOrElse("ALL")}targets"
    val sampleFile = s"$out/megacell_${nrCells.getOrElse("ALL")}cells_${nrTargets.getOrElse("ALL")}targets.cell-ids.txt"
    val infoFile   = s"$out/megacell_${nrCells.getOrElse("ALL")}cells_${nrTargets.getOrElse("ALL")}targets.info.txt"

    println(parsedArgs)
    writeToFile(infoFile, parsedArgs)

    deleteDirectory(outDir)
    
    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache
    val TFs = readTFs(mouseTFs).toSet

    val totalCellCount = ds.head.values.size

    val cellIndicesSubset = nrCells.map(nr => randomSubset(nr, 0 until totalCellCount))

    cellIndicesSubset.foreach(subset => writeToFile(sampleFile, subset.sorted.mkString("\n")))

    val slicedByCells =
      cellIndicesSubset
        .map(subset => ds.slice(subset).cache)
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
          params = params.copy(
            nrRounds = nrRounds,
            boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
          targets = targetSet,
          nrPartitions = Some(nrPartitions))
        .cache

    regulations
      .normalize(params)
      .saveTxt(outDir)
  }

}