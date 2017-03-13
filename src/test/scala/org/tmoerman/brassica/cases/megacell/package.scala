package org.tmoerman.brassica.cases

import org.tmoerman.brassica.util.PropsReader._

/**
  * @author Thomas Moerman
  */
package object megacell {

  def megacell = props("megacell")

  def megacellRowsParquet    = props("megacellRowsParquet")
  def megacellColumnsParquet = props("megacellColumnsParquet")

  def mouseTFs = props("mouseTFs")

}