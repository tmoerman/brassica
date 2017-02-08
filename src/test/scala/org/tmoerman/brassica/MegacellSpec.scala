package org.tmoerman.brassica

import org.scalatest.{Matchers, FlatSpec}

import breeze.linalg._

import org.apache.spark.ml.feature.{ LabeledPoint => MLLabeledPoint }

/**
  * @author Thomas Moerman
  */
class MegacellSpec extends FlatSpec with Matchers {

  val path = "/Users/tmo/work/ghb2016/data/bigsc/1M_neurons_filtered_gene_bc_matrices_h5.h5"

  it should "parse 1 column correctly" in {
    val csc1: CSCMatrix[Int] = Megacell(path, n = Some(1)).get

    val e1 = csc1.activeValuesIterator.take(20).toList
  }

}