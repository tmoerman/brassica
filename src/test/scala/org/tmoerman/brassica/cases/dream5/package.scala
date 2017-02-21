package org.tmoerman.brassica.cases

import org.tmoerman.brassica.util.PropsReader._

/**
  * @author Thomas Moerman
  */
package object dream5 {

  def wd = props("dream5")

  def ecoliData  = wd + "Ecoli/ecoli_data.tsv"
  def ecoliGenes = wd + "Ecoli/ecoli_gene_names.tsv"
  def ecoliTFs   = wd + "Ecoli/ecoli_tf_names.tsv"

  def saureusData  = wd + "Saureus/saureus_data.tsv"
  def saureusGenes = wd + "Saureus/saureus_gene_names.tsv"
  def saureusTFs   = wd + "Saureus/saureus_tf_names.tsv"

  def yeastData  = wd + "Scerevisiae/yeast_data.tsv"
  def yeastGenes = wd + "Scerevisiae/yeast_gene_names.tsv"
  def yeastTFs   = wd + "Scerevisiae/yeast_tf_names.tsv"

}