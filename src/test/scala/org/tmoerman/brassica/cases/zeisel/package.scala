package org.tmoerman.brassica.cases

import org.tmoerman.brassica.util.PropsReader._

/**
  * @author Thomas Moerman
  */
package object zeisel {

  def zeiselMrna = props("zeisel")

  def zeiselFiltered = props("zeiselFiltered")

  def zeiselParquet = props("zeiselParquet")

  def mouseTFs = props("mouseTFs")

}
