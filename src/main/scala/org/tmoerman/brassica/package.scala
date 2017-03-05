package org.tmoerman

/**
  * @author Thomas Moerman
  */
package object brassica {

  // type aliases facilitate code interpretation
  type Count = Int
  type Index = Long
  type Gene  = String
  type BoosterParams = Map[String, Any]

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

  case class XGBoostParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                           nrRounds: Int = 10,
                           nrWorkers: Option[Int] = Some(1))

}