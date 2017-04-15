package org.tmoerman.brassica.util

import breeze.linalg.{CSCMatrix, SliceMatrix, SparseVector}
import breeze.linalg.SparseVector._
import breeze.storage.Zero
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.DMatrix
import org.tmoerman.brassica.Expression
import org.tmoerman.brassica.util.TimeUtils.{pretty, profile}

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

  /**
    * @param sliced A Breeze SliceMatrix.
    * @return Returns an XGBoost DMatrix.
    */
  def toDMatrix(sliced: SliceMatrix[Int, Int, Expression]): DMatrix = {
    val (csc, duration) = profile {
      val builder = new CSCMatrix.Builder[Expression](rows = sliced.rows, cols = sliced.cols)

      sliced
        .activeIterator
        .filter(_._2 != 0f)
        .foldLeft(builder){
          case (bldr, ((r, c), v)) => bldr.add(r, c, v); bldr }
        .result
    }

    println(s"creating CSCMatrix from SliceMatrix took ${pretty(duration)}")

    toDMatrix(csc)
  }

  /**
    * @param csc A Breeze CSCMatrix.
    * @return Returns an XGBoost DMatrix.
    */
  def toDMatrix(csc: CSCMatrix[Expression]): DMatrix =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data, CSC)

}