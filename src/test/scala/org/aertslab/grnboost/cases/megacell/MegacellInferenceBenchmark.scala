package org.aertslab.grnboost.cases.megacell

import org.aertslab.grnboost.DataReader.readRegulators
import org.aertslab.grnboost.Specs.Server
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost._
import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class MegacellInferenceBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

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

  "Megacell Gene Network Inference" should "work on a lot of cells" taggedAs Server in {
    inferSub(250000)
  }

  private def inferSub(nrCells: CellCount) = {
    import spark.implicits._

    val full = props("megacellFull")
    val out  = props("megacellOut")

    val TFs = readRegulators(mouseTFs).toSet

    val ds = spark.read.parquet(full).as[ExpressionByGene]

    val totalCellCount = ds.head.values.size

    val subset = randomSubset(nrCells, 0 until totalCellCount)

    val sliced = ds.slice(subset)

    val nrCores = spark.sparkContext.defaultParallelism
    val nrThreadsPerRegression = 11
    val nrTargets = 200

    val regulationsDS =
      GRNBoost
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