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

  val TARGET_GENE_INDEX         = "target_index"
  val TARGET_GENE_NAME          = "target_name"
  val CANDIDATE_REGULATOR_INDEX = "regulator_index"
  val CANDIDATE_REGULATOR_NAME  = "regulator_name"
  val IMPORTANCE                = "importance"

}