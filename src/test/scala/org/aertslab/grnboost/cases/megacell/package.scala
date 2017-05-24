package org.aertslab.grnboost.cases

import org.aertslab.grnboost.util.PropsReader._

/**
  * @author Thomas Moerman
  */
package object megacell {

  def megacell = props("megacell")

  def megacellRowsParquet    = props("megacellRowsParquet")
  def megacellColumnsParquet = props("megacellColumnsParquet")

  def mouseTFs = props("mouseTFs")

}