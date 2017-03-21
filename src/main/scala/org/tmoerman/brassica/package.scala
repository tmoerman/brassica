package org.tmoerman

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

  type CellIndex = Int
  type CellCount = Int
  type GeneIndex = Int
  type GeneCount = Int
  type Expression = Int
  type Importance = Int

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

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    "silent" -> 1
  )

  /**
    * @param gene The gene name.
    * @param values The sparse expression vector.
    */
  case class ExpressionByGene(gene: Gene, values: MLVector) { // TODO rename values -> "expression"
    def response = values.toArray.map(_.toFloat)
  }

  /**
    * Implicit pimp class for adding functions to Dataset[ExpressionByGene]
    * @param ds
    */
  implicit class ExpressionByGeneFunctions(val ds: Dataset[ExpressionByGene]) {
    import ds.sparkSession.implicits._

    def genes = ds.select($"gene").rdd.map(_.getString(0)).collect.toList

    def slice(cellIndices: Seq[CellIndex]): Dataset[ExpressionByGene] = ??? // FIXME implement this
  }

  /**
    * @param predictor
    * @param index
    */
  case class PredictorToIndex(predictor: Gene, index: GeneIndex)

  /**
    * @param regulator The regulator gene name.
    * @param target The target gene name.
    * @param importance The inferred importance of the regulator vis-a-vis the target.
    */
  case class Regulation(regulator: Gene, target: Gene, importance: Importance)

  val DEFAULT_NR_BOOSTING_ROUNDS = 50
  val DEFAULT_NR_FOLDS = 10

  /**
    * @param boosterParams The XGBoost Map of booster parameters.
    * @param nrRounds The nr of boosting rounds.
    * @param nrFolds The nr of cross validation folds.
    */
  case class RegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                              nrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS,
                              nrFolds: Int = DEFAULT_NR_FOLDS)

}