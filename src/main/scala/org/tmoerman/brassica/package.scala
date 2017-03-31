package org.tmoerman

import com.eharmony.spotz.optimizer.hyperparam.{RandomSampler, UniformDouble, UniformInt}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.Dataset

/**
  * Constants, case classes and type aliases.
  *
  * @author Thomas Moerman
  */
package object brassica {

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
  type Expression = Float
  type Importance = Float

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

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    "silent" -> 1
  )

  val DEFAULT_BOOSTER_PARAM_SPACE: BoosterParamSpace = Map(
    // model complexity
    "max_depth"        -> UniformInt(3, 10),
    "min_child_weight" -> UniformDouble(1, 15),
    "gamma"            -> UniformDouble(0, 15),

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
    def response: Array[Float] = values.toArray.map(_.toFloat)
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
    def slice(cellIndices: Seq[CellIndex]): Dataset[ExpressionByGene] = {
      val slicer =
        new VectorSlicer()
          .setInputCol("values")
          .setOutputCol("sliced")
          .setIndices(cellIndices.toArray)

      Some(ds)
        .map(slicer.transform)
        .map(_.select($"gene", $"sliced"))
        .map(_.withColumnRenamed("sliced", "values"))
        .map(_.as[ExpressionByGene])
        .get
    }

  }

  /**
    * @param regulator The regulator gene name.
    * @param target The target gene name.
    * @param importance The inferred importance of the regulator vis-a-vis the target.
    */
  case class Regulation(regulator: Gene, target: Gene, importance: Importance)

  /**
    * @param target The target gene name.
    * @param metric The evaluation metric, e.g. RMSE.
    */
  case class OptimizedHyperParams(target: Gene, metric: String) // TODO nested data structure for hyperparams

  /**
    * @param boosterParams The XGBoost Map of booster parameters.
    * @param nrRounds The nr of boosting rounds.
    */
  case class XGBoostRegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                                     nrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS)

  /**
    * @param boosterParamSpace The space of booster parameters to search through for an optimal set.
    * @param nrFolds The nr of cross validation folds in which to splice the training data.
    * @param nrRounds The nr of boosting rounds.
    * @param parallel Whether to run the optimization per partition in parallel or not
    *                 (mostly not because we run multiple optimizations in parallel).
    */
  case class XGBoostOptimizationParams(boosterParamSpace: BoosterParamSpace = DEFAULT_BOOSTER_PARAM_SPACE,
                                       nrFolds: Int = DEFAULT_NR_FOLDS,
                                       nrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS,
                                       parallel: Boolean = false)

}