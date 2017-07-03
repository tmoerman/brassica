package org.aertslab

import com.eharmony.spotz.optimizer.hyperparam.{RandomSampler, UniformDouble, UniformInt}
import org.aertslab.grnboost.util.TriangleRegularization._
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.Dataset

import scala.util.Random

/**
  * Application wide domain classes, constants and type aliases.
  *
  * @author Thomas Moerman
  */
package object grnboost {

  val GRN_BOOST = "GRNBoost"

  type Path = String

  type Count = Int
  type Index = Long
  type Gene  = String

  type BoosterParams     = Map[String, Any]
  type BoosterParamSpace = Map[String, RandomSampler[_]]

  type CellIndex = Int
  type CellCount = Int
  type GeneIndex = Int
  type GeneCount = Int
  type Round     = Int
  type Seed      = Int

  type Expression = Float
  type Importance = Float
  type Loss       = Float

  type Frequency = Int
  type Gain      = Float
  type Cover     = Float

  type TreeDump  = String
  type ModelDump = Seq[TreeDump]

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

  val DEFAULT_MAX_BOOSTING_ROUNDS = 5000

  val DEFAULT_ESTIMATION_SET = 20
  val DEFAULT_NR_FOLDS       = 5
  val DEFAULT_NR_TRIALS      = 1000L
  val DEFAULT_SEED           = 666
  val DEFAULT_EVAL_METRIC    = "rmse"

  val XGB_THREADS = "nthread"
  val XGB_SILENT  = "silent"
  val XGB_ETA     = "eta"
  val XGB_SEED    = "seed"
  val XGB_METRIC  = "eval_metric"
  val XGB_MAX_DEPTH  = "max_depth"

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    XGB_SILENT    -> 1,
    XGB_THREADS   -> 1,
    XGB_ETA       -> 0.1,
    XGB_MAX_DEPTH -> 3
  )

  implicit class BoosterParamsFunctions(boosterParams: BoosterParams) {

    def withDefaults: BoosterParams =
      DEFAULT_BOOSTER_PARAMS
        .foldLeft(boosterParams){ case (params, (k, v)) =>
          if (params contains k) params else params updated (k, v) }

    def withSeed(seed: Seed): BoosterParams =
      boosterParams.updated("seed", seed)

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
    def response: Array[Expression] = values.toArray.map(_.toFloat)
  }

  /**
    * Implicit pimp class for adding functions to Dataset[ExpressionByGene]
    * @param ds The Dataset of ExpressionByGene instances to pimp.
    */
  implicit class ExpressionByGeneDatasetFunctions(val ds: Dataset[ExpressionByGene]) {
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

    /**
      * @param sampleSize
      * @return Returns the sample indices and the sampled Dataset.
      */
    def subSample(sampleSize: Int): (Option[Seq[CellIndex]], Dataset[ExpressionByGene]) = {
      val count = ds.head.values.size

      if (sampleSize >= count) (None, ds)
      else {
        val subset = randomSubset(sampleSize, 0 until count)

        (Some(subset), ds.slice(subset).cache)
      }
    }

  }

  /**
    * Raw XGBoost regression output data structure.
    *
    * @param regulator
    * @param target
    * @param gain
    * @param include
    */
  case class Regulation(regulator: Gene,
                        target: Gene,
                        gain: Gain,
                        include: Int = 1) {

    def mkString(d: String = "\t") = productIterator.mkString(d)

  }

  /**
    * @param fold
    * @param target
    * @param loss
    * @param rounds
    */
  case class RoundsEstimation(fold: Int,
                              target: Gene,
                              loss: Loss,
                              rounds: Int)

  /**
    * Training and test loss by boosting round.
    *
    * @param target The target gene.
    * @param train The training loss.
    * @param test The test loss.
    * @param round The boosting round.
    */
  case class LossByRound(target: Gene, train: Loss, test: Loss, round: Int) {

    def mkString(d: String = "\t") = productIterator.mkString(d)

  }

  /**
    * Implicit pimp class.
    * @param ds
    */
  implicit class RegulationDatasetFunctions(val ds: Dataset[Regulation]) {

    import ds.sparkSession.implicits._

    /**
      * @param params
      * @return Returns the Dataset with regularization labels calculated with the Triangle method.
      */
    def withRegularizationLabels(params: XGBoostRegressionParams): Dataset[Regulation] =
      params
        .regularize
        .map(precision =>
          ds
            .rdd
            .groupBy(_.target)
            .values
            .flatMap(_.toList match {
              case Nil  => Nil
              case list =>
                val sorted = list.sortBy(-_.gain)
                val gains = sorted.map(_.gain)

                (sorted zip labels(gains, precision)).map{ case (reg, label) => reg.copy(include = label) }
            })
            .toDS
        )
        .getOrElse(ds)

    /**
      * Save the Dataset as a text file with specified delimiter
      * @param path Target file path.
      * @param includeLabel Include the label.
      * @param delimiter Default tab.
      */
    def saveTxt(path: Path, includeLabel: Boolean = true, delimiter: String = "\t"): Unit = {
      val nr = if (includeLabel) 4 else 3

      ds
        .rdd
        .map(_.productIterator.take(nr).mkString(delimiter))
        .repartition(1)
        .saveAsTextFile(path)
    }

  }

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

  sealed trait FeatureImportanceMetric
  case object GAIN  extends FeatureImportanceMetric
  case object COVER extends FeatureImportanceMetric
  case object FREQ  extends FeatureImportanceMetric

  /**
    * Data structure holding parameters for XGBoost regression.
    *
    * @param boosterParams The XGBoost Map of booster parameters.
    * @param nrRounds The nr of boosting rounds.
    * @param nrFolds The nr of folds in CV packs.
    * @param regularize Whether to use the L-curve cutoff strategy if Some. Contains threshold parameter.
    */
  case class XGBoostRegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                                     nrRounds: Int,
                                     nrFolds: Int = DEFAULT_NR_FOLDS,
                                     regularize: Option[Double] = Some(DEFAULT_PRECISION)) {

    def seed = boosterParams.get("seed").map(_.toString.toInt).getOrElse(DEFAULT_SEED)

  }

  /**
    * Early stopping parameter, for stopping boosting rounds when the delta in loss values is smaller than the
    * specified delta, over a window of boosting rounds of specified size. The boosting round halfway of the window
    * is returned as final result.
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
    * @param nrTrials The number of random search trials per batch. Typically one batch per target is used,
    *                         and batches are parallelized in different partitions.
    * @param nrFolds The nr of cross validation folds in which to splice the training data.
    * @param maxNrRounds The maximum number of boosting rounds.
    * @param earlyStopParams Optional early stopping parameters.
    * @param seed The seed for computing the random n folds.
    * @param onlyBestTrial Specifies whether to return only the best trial or all trials for a target gene.
    */
  case class XGBoostOptimizationParams(boosterParamSpace: BoosterParamSpace = DEFAULT_BOOSTER_PARAM_SPACE,
                                       extraBoosterParams: BoosterParams = Map.empty,
                                       evalMetric: String = DEFAULT_EVAL_METRIC,
                                       nrTrials: Int = 1000,
                                       nrFolds: Int = DEFAULT_NR_FOLDS,

                                       maxNrRounds: Int = DEFAULT_MAX_BOOSTING_ROUNDS,
                                       earlyStopParams: Option[EarlyStopParams] = Some(EarlyStopParams()),

                                       seed: Seed = DEFAULT_SEED,
                                       onlyBestTrial: Boolean = true) {

    assert(nrFolds > 0, s"nr folds must be greater than 0 (specified: $nrFolds) ")

  }

  /**
    * @param seed The random seed.
    * @return Returns a new Random initialized with a seed.
    */
  def random(seed: Long): Random = {
    val rng = new Random(seed)
    rng.nextInt // get rid of first, low entropy
    rng
  }

  /**
    * @param keep The amount to keep from the range.
    * @param range The cell index range to choose from.
    * @param seed A random seed.
    * @return Returns the random subset.
    */
  def randomSubset(keep: Count, range: Range, seed: Seed = DEFAULT_SEED): Seq[CellIndex] = {
    val cellIndices: Seq[CellIndex] = range

    if (keep < range.size)
      random(seed).shuffle(cellIndices).take(keep).sorted
    else
      random(seed).shuffle(cellIndices).sorted
  }

  implicit class ProductFunctions(p: Product) {

    def toMap =
      p
        .getClass.getDeclaredFields.map(_.getName)
        .zip(p.productIterator.to)
        .toMap

  }

}