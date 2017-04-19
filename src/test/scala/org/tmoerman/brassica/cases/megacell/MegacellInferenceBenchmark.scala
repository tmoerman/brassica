package org.tmoerman.brassica.cases.megacell

import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.DataReader
import org.tmoerman.brassica.cases.DataReader.readTFs
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{ExpressionByGene, XGBoostRegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class MegacellInferenceBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParamsBio = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 6,
    "max_depth" -> 6
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 12,
      boosterParams = boosterParamsBio)

  val full = props("megacellFull")
  val out  = props("megacellOut")

  "Megacell Gene Network Inference" should "work on a lot of cells" in {
    inferSub(250000)
  }

  private def inferSub(nrCells: CellCount) = {
    import spark.implicits._

    val TFs = readTFs(mouseTFs).toSet

    val ds = spark.read.parquet(full).as[ExpressionByGene]

    val totalCellCount = ds.head.values.size

    val subset = randomSubset(nrCells, 0 until totalCellCount)

    val sliced = ds.slice(subset)

    val nrCores = spark.sparkContext.defaultParallelism
    val nrThreadsPerRegression = 11
    val nrTargets = 200

    val regulationsDS =
      ScenicPipeline
        .inferRegulations(
          sliced,
          candidateRegulators = TFs,
          params = params.copy(boosterParams = params.boosterParams + (XGB_THREADS -> nrThreadsPerRegression)),
          targets = sliced.take(nrTargets).map(_.gene).toSet,
          nrPartitions = Some(nrCores / nrThreadsPerRegression))
        .cache()

    regulationsDS
      .write
      .mode(Overwrite)
      .parquet(out + s"subset_${nrCells}cells_${nrTargets}targets")
  }
}