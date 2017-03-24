package org.tmoerman.brassica.cases

import org.tmoerman.brassica.util.PropsReader._

/**
  * @author Thomas Moerman
  */
package object dream5 {

  def wd = props("dream5Training")

  def network = Map(
    1 -> (inSilicoData, inSilicoTFs),
    2 -> (saureusData,  saureusTFs),
    3 -> (ecoliData,    ecoliTFs),
    4 -> (yeastData,    yeastTFs)
  )

  val nw1 = "Network 1 - in silico"
  val nw2 = "Network 2 - S. aureus"
  val nw3 = "Network 3 - E. coli"
  val nw4 = "Network 4 - S. cerevisiae"

  def inSilicoData  = s"$wd$nw1/net1_expression_data.tsv"
  def inSilicoTFs   = s"$wd$nw1/net1_transcription_factors.tsv"

  def saureusData  = s"$wd$nw2/net2_expression_data.tsv"
  def saureusTFs   = s"$wd$nw2/net2_transcription_factors.tsv"

  def ecoliData  = s"$wd$nw3/net3_expression_data.tsv"
  def ecoliTFs   = s"$wd$nw3/net3_transcription_factors.tsv"

  def yeastData  = s"$wd$nw4/net4_expression_data.tsv"
  def yeastTFs   = s"$wd$nw4/net4_transcription_factors.tsv"

}