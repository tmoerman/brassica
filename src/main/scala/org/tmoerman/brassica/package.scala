package org.tmoerman

/**
  * @author Thomas Moerman
  */
package object brassica {

  // type aliases facilitate code interpretation
  type Count = Int
  type Index = Long
  type Gene = String
  type XGBoostParams = Map[String, Any]

  val EXPRESSION_VECTOR = "expression"

}