package org.aertslab.grnboost.cases.megacell

import com.eharmony.spotz.optimizer.hyperparam.UniformDouble
import org.aertslab.grnboost.DataReader.readRegulators
import org.aertslab.grnboost.Specs.Server
import org.aertslab.grnboost._
import org.aertslab.grnboost.algo.OptimizeXGBoostHyperParams.Constantly
import org.aertslab.grnboost.util.PropsReader.props
import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class MegacellOptimizationBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val minChildDepthSpace: BoosterParamSpace = Map(
    // model complexity
    "max_depth"        -> Constantly(6),
    "min_child_weight" -> UniformDouble(1, 15),

    // robustness to noise
    "subsample"        -> Constantly(0.8d),
    "colsample_bytree" -> Constantly(0.8d),
    
    // learning rate
    "eta"              -> Constantly(0.15d)
  )

  val optimizationParams =
    XGBoostOptimizationParams(
      boosterParamSpace = minChildDepthSpace,
      nrTrials = 5,
      maxNrRounds = 20,
      //earlyStopParams = None,
      //nrBatches = 1,
      onlyBestTrial = false
    )

  val targets = Set("Sox10", "Tgfbi", "Gm11266", "Akirin1", "Abcb7", "Clca3b", "Yipf3")

  "Megacell min_child_depth and rounds optimization on many cells" should "work" taggedAs Server ignore {
    optimizeSub(100000)
  }

  private def optimizeSub(nrCells: CellCount) = {
    import spark.implicits._

    val full         = props("megacellFull")
    val optimization = props("megacellOptimization")

    val TFs = readRegulators(mouseTFs).toSet

    val expressionsByGene = spark.read.parquet(full).as[ExpressionByGene]

    val totalCellCount = expressionsByGene.head.values.size

    val subset_10k = randomSubset(nrCells, 0 until totalCellCount)

    val sliced = expressionsByGene.slice(subset_10k).cache

    val nrCores = spark.sparkContext.defaultParallelism
    val nrThreadsPerRegression = 44

    val hyperParamsLossDS =
      GRNBoost
        .optimizeHyperParams(
          expressionsByGene = sliced,
          candidateRegulators = TFs,
          params = optimizationParams.copy(extraBoosterParams = Map(XGB_THREADS -> nrThreadsPerRegression)),
          targets = targets,
          nrPartitions = Some(nrCores / nrThreadsPerRegression))
        .cache()

    hyperParamsLossDS
      .write
      .mode(Overwrite)
      .parquet(optimization + s"subset_$nrCells")
  }

}