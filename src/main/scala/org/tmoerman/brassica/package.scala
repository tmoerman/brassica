package org.tmoerman

/**
  * @author Thomas Moerman
  */
package object brassica {

  // type aliases facilitate code interpretation

  type Path = String

  type Count = Int
  type Index = Long
  type Gene  = String
  type BoosterParams = Map[String, Any]

  type CellIndex = Int
  type CellCount = Int
  type GeneIndex = Int
  type GeneCount = Int
  type GeneExpression = Int
  type Importance = Float

  val EXPRESSION_VECTOR    = "expression"
  val CANDIDATE_REGULATORS = "regulators"
  val TARGET_GENE          = "target_gene"

  val TARGET_GENE_INDEX         = "target_index"
  val TARGET_GENE_NAME          = "target_name"
  val CANDIDATE_REGULATOR_INDEX = "regulator_index"
  val CANDIDATE_REGULATOR_NAME  = "regulator_name"
  val IMPORTANCE                = "importance"

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    "silent" -> 1
  )

  case class RegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                              nrRounds: Int = 10,
                              normalize: Boolean = true,
                              nrWorkers: Option[Int] = Some(1),
                              verbose: Boolean = false)

}