package org.tmoerman

import com.eharmony.spotz.optimizer.hyperparam.{RandomSampler, UniformDouble, UniformInt}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.Dataset

import scala.util.Random

/**
  * Application wide domain classes, constants and type aliases.
  *
  * @author Thomas Moerman
  */
package object brassica {

  val GRN_BOOST = "GRNBoost"

  type Path = String

  type Count = Int
  type Index = Long
  type Gene  = String

  type BoosterParams = Map[String, Any]
  type BoosterParamSpace = Map[String, RandomSampler[_]]

  type CellIndex = Int
  type CellCount = Int
  type GeneIndex = Int
  type GeneCount = Int
  type Round     = Int

  type Expression = Float
  type Importance = Float
  type Loss       = Float

  val VALUES      = "values"
  val GENE        = "gene"
  val EXPRESSION  = "expression"
  val REGULATORS  = "regulators"
  val TARGET_GENE = "target_gene"

  val TARGET_INDEX    = "target_index"
  val TARGET_NAME     = "target_name"
  val REGULATOR_INDEX = "regulator_index"
  val REGULATOR_NAME  = "regulator_name"
  val IMPORTANCE      = "importance"

  val DEFAULT_NR_BOOSTING_ROUNDS = 100

  val DEFAULT_NR_FOLDS           = 10
  val DEFAULT_NR_TRIALS          = 1000L
  val DEFAULT_SEED               = 666L
  val DEFAULT_EVAL_METRIC        = "rmse"

  val XGB_THREADS = "nthread"
  val XGB_SILENT  = "silent"

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    XGB_SILENT -> 1
  )

  implicit class BoosterParamsFunctions(boosterParams: BoosterParams) {

    def withDefaults: BoosterParams =
      Some(boosterParams)
        .map(p => if (p contains XGB_THREADS) p else p + (XGB_THREADS -> 1))
        .map(p => if (p contains XGB_SILENT)  p else p + (XGB_SILENT -> 1))
        .get

  }

  val DEFAULT_BOOSTER_PARAM_SPACE: BoosterParamSpace = Map(
    // model complexity
    "max_depth"        -> UniformInt(3, 10),
    "min_child_weight" -> UniformDouble(1, 15),

    // robustness to noise
    "subsample"        -> UniformDouble(0.5, 1.0),
    "colsample_bytree" -> UniformDouble(0.5, 1.0),

    // learning rate
    "eta"              -> UniformDouble(0.01, 0.2)
  )

  /**
    * @param gene The gene name.
    * @param values The sparse expression vector.
    */
  case class ExpressionByGene(gene: Gene, values: MLVector) { // TODO rename values -> "expression"
    def response: Array[Expression] = values.toArray.map(_.toFloat) // TODO test case
  }

  /**
    * Implicit pimp class for adding functions to Dataset[ExpressionByGene]
    * @param ds The Dataset to pimp.
    */
  implicit class ExpressionByGeneFunctions(val ds: Dataset[ExpressionByGene]) {
    import ds.sparkSession.implicits._

    /**
      * @return Returns the genes in the Dataset as List of Strings.
      */
    def genes: List[Gene] = ds.select($"gene").rdd.map(_.getString(0)).collect.toList

    /**
      * @param cellIndices The cells to slice from the Dataset.
      * @return Returns the Dataset with values sliced in function of the specified Seq of cell indices.
      */
    def slice(cellIndices: Seq[CellIndex]): Dataset[ExpressionByGene] =
      new VectorSlicer()
        .setInputCol("values")
        .setOutputCol("sliced")
        .setIndices(cellIndices.toArray)
        .transform(ds)
        .select($"gene", $"sliced".as("values"))
        .as[ExpressionByGene]

    import org.apache.spark.ml.linalg.BreezeMLConversions._

    def standardized: Dataset[ExpressionByGene] =
      ds.map(e => {

        import breeze.stats._

        val v = e.values.br

        val mu = mean.apply(v)
        val s  = stddev.apply(v)

        val result = (v - mu) / s

        e.copy(values = result.ml)
      })

  }

  /**
    * XGBoost regression output data structure.
    *
    * @param regulator The regulator gene name.
    * @param target The target gene name.
    * @param importance The inferred importance of the regulator vis-a-vis the target.
    */
  case class Regulation(regulator: Gene,
                        target: Gene,
                        importance: Importance)

  /**
    * @param target The target gene name.
    * @param metric The evaluation metric, e.g. RMSE.
    * @param rounds A putative sufficient nr of boosting rounds ~ to be computed with early stopping.
    * @param loss The loss function value.
    *
    *
    */
  case class HyperParamsLoss(target: Gene,
                             metric: String,
                             rounds: Round,
                             loss: Loss,
                             max_depth: Int,
                             min_child_weight: Double,
                             subsample: Double,
                             colsample_bytree: Double,
                             eta: Double) {

    def toBoosterParams: BoosterParams = ???

  }

  /**
    * Data structure holding parameters for XGBoost regression.
    *
    * @param boosterParams The XGBoost Map of booster parameters.
    * @param nrRounds The nr of boosting rounds.
    */
  case class XGBoostRegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                                     nrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS)

  /**
    * Early stopping parameter, for stopping boosting rounds when the delta in loss values is smaller than the specified
    * delta, over a window of boosting rounds of specified size. The boosting round halfway of the window is returned as
    * final result.
    *
    * @param size The size of the window.
    * @param lossDelta The loss delta over the window.
    */
  case class EarlyStopParams(size: Int = 10, lossDelta: Float = 0.01f)

  /**
    * Data structure holding parameters for XGBoost regression optimization.
    *
    * @param boosterParamSpace The space of booster parameters to search through for an optimal set.
    * @param evalMetric The n-fold evaluation metric, default "rmse".
    * @param nrTrialsPerBatch The number of random search trials per batch. Typically one batch per target is used,
    *                         and batches are parallelized in different partitions.
    * @param nrBatches The number of batches. Increase when partitioning trials for the same target.
    * @param nrFolds The nr of cross validation folds in which to splice the training data.
    * @param maxNrRounds The maximum number of boosting rounds.
    * @param earlyStopParams Optional early stopping parameters.
    * @param seed The seed for computing the random n folds.
    * @param onlyBestTrial Specifies whether to return only the best trial or all trials for a target gene.
    */
  case class XGBoostOptimizationParams(boosterParamSpace: BoosterParamSpace = DEFAULT_BOOSTER_PARAM_SPACE,
                                       extraBoosterParams: BoosterParams = Map.empty,
                                       evalMetric: String = DEFAULT_EVAL_METRIC,
                                       nrTrialsPerBatch: Int = 1000,
                                       nrBatches: Int = 1,
                                       nrFolds: Int = DEFAULT_NR_FOLDS,

                                       maxNrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS,
                                       earlyStopParams: Option[EarlyStopParams] = Some(EarlyStopParams()),

                                       seed: Long = DEFAULT_SEED,
                                       onlyBestTrial: Boolean = true) {

    assert(nrFolds > 0, s"nr folds must be greater than 0 (specified: $nrFolds) ")

  }

  /**
    * @param seed The random seed.
    * @return Returns a new Random initialized with a seed.
    */
  def random(seed: Long = DEFAULT_SEED): Random = {
    val rng = new Random(seed)
    rng.nextLong // get rid of first, low entropy}
    rng
  }

  /**
    * @param keep The amount to keep from the range.
    * @param range The range to choose from.
    * @param seed A random seed.
    * @return Returns the random subset.
    */
  def randomSubset(keep: Count, range: Range, seed: Long = DEFAULT_SEED): Seq[CellIndex] = {
    // TODO default to all if condition violated
    assert(keep <= range.size, s"keep ($keep) should be smaller than or equal to range size (${range.size})")

    val all: Seq[CellIndex] = range

    random(seed).shuffle(all).take(keep).sorted
  }

}