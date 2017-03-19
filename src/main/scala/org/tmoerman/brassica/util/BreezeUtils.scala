package org.tmoerman.brassica.util

import breeze.linalg.{CSCMatrix, SparseVector}
import breeze.linalg.SparseVector._
import breeze.storage.Zero

import scala.reflect.ClassTag

/**
  * @author Thomas Moerman
  */
object BreezeUtils {

  /**
    * @param cols The number of columns in the CSCMatrix
    * @param idx The column index for the specified SparseVector in the resulting CSCMatrix.
    * @param v The SparseVector to insert at position idx.
    * @tparam T Generic numeric type
    * @return Returns a CSCMatrix with cols columns and rows equal to the length of the specified SparseVector.
    */
  def asCSCMatrix[@specialized(Double, Int, Float, Long) T:ClassTag:Zero](cols: Int, idx: Int, v: SparseVector[T]): CSCMatrix[T] = {
    val L = idx
    val R = cols - idx - 1

    val L_0s = List.tabulate(L)(_ => zeros[T](v.length))
    val R_0s = List.tabulate(R)(_ => zeros[T](v.length))

    horzcat(L_0s ::: v :: R_0s: _*)
  }

}