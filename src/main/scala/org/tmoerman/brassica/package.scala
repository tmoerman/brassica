package org.tmoerman

import org.apache.spark.ml.linalg.{Vector => MLVector}

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

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    "silent" -> 1
  )

  /**
    * @param gene The gene name.
    * @param values The sparse expression vector.
    */
  case class ExpressionByGene(gene: Gene, values: MLVector) // TODO rename values -> "expression"

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

  case class RegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                              nrRounds: Int = 10,
                              normalize: Boolean = true,
                              nrWorkers: Option[Int] = Some(1),
                              showCV: Boolean = false)

}