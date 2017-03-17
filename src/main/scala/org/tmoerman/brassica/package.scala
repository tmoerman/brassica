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

  case class RegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                              nrRounds: Int = 10,
                              normalize: Boolean = true,
                              nrWorkers: Option[Int] = Some(1),
                              verbose: Boolean = false)

}