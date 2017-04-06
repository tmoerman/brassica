package org.tmoerman.brassica.util

import breeze.linalg.CSCMatrix
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.Expression

/**
  * @author Thomas Moerman
  */
class BreezeUtilsSpec extends FlatSpec with Matchers {

  // TODO specify test correctly
  "toDMatrix" should "work correctly" in {

    val builder = new CSCMatrix.Builder[Expression](rows = 10, cols = 4)

    builder.add(0, 0, 6f)
    builder.add(1, 1, 7f)
    builder.add(2, 2, 8f)
    builder.add(3, 3, 9f)

    val csc = builder.result

    val sliced = csc(0 until 10, Seq(1, 2))
    val dense = sliced.toDenseMatrix

    val arrSliced = sliced.activeValuesIterator.toList

    // TODO 1 of these is obviously wrong!!
    println(sliced.rows, sliced.cols, sliced.activeValuesIterator.toList)
    println(dense.rows, dense.cols, dense.data.toList)

    val dm = BreezeUtils.toDMatrix(sliced)
    //dm.setLabel(Array(1, 1, 1, 1, 0, 0, 0, 0, 0, 0))

    println(dm.toString)

    dm.delete()
  }

}