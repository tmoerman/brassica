package org.tmoerman

/**
  * @author Thomas Moerman
  */
package object brassica {

  // type aliases facilitate code interpretation
  type Count = Int
  type Index = Long
  type Gene  = String
  type XGBoostParams = Map[String, Any]

  val EXPRESSION_VECTOR    = "expression"
  val TARGET_GENE          = "target"
  val CANDIDATE_REGULATORS = "regulators"
  val CANDIDATE_REGULATOR  = "regulator"
  val IMPORTANCE           = "importance"

}