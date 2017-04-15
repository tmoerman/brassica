package org.tmoerman.brassica.cases.megacell

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica.{ExpressionByGene, ScenicPipeline, XGB_THREADS, XGBoostRegressionParams, randomSubset}

/**
  * @author Thomas Moerman
  */
object MegacellInference {

  val boosterParamsBio = Map(
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
      boosterParams = boosterParamsBio)

  def main(args: Array[String]): Unit = {
    // FIXME better arg parsing
    val parquet   = args(0)
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
        |* parquet         = $parquet
        |* mouseTFs        = $mouseTFs
        |* output          = $out
        |* nr cells        = $nrCells
        |* nr target genes = $nrTargets
        |* nr xgb threads  = $nrThreads
      """.stripMargin

    println(parsed)

    val outDir = s"$out/subset_${nrCells.getOrElse("ALL")}cells_${nrTargets.getOrElse("ALL")}targets"

    FileUtils.deleteDirectory(new File(outDir))

    val spark =
      SparkSession
        .builder
        .master("local[*]")
        .appName("GRADINETS")
        .getOrCreate()

    import spark.implicits._

    val ds = spark.read.parquet(parquet).as[ExpressionByGene].cache
    val TFs = MegacellReader.readTFs(mouseTFs).toSet

    val totalCellCount = ds.head.values.size

    val slicedByCells =
      nrCells
        .map(nr => {
          val subset = randomSubset(nr, 0 until totalCellCount)
          ds.slice(subset).cache
        })
        .getOrElse(ds)

    val targetSet = nrTargets.map(nr => slicedByCells.take(nr).map(_.gene).toSet).getOrElse(Set.empty)

    val regulations =
      ScenicPipeline
        .inferRegulations(
          slicedByCells,
          candidateRegulators = TFs,
          params = params.copy(nrRounds = nrRounds, boosterParams = params.boosterParams + (XGB_THREADS -> nrThreads)),
          targets = targetSet,
          nrPartitions = Some(nrPartitions))
        .cache

    regulations
      .repartition(1)
      .rdd
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(outDir)
  }

}