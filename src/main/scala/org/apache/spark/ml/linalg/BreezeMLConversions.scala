package org.apache.spark.ml.linalg

import breeze.linalg.{Matrix => BreezeMatrix, Vector => BreezeVector, CSCMatrix}
import org.apache.spark.ml.linalg.{Vector => MLVector, Matrix => MLMatrix}

import scala.collection.mutable.ArrayBuilder

/**
  * @author Thomas Moerman
  */
object BreezeMLConversions {

  implicit class MLLibVectorConversion(val vector: MLVector) extends AnyVal {
    def br: BreezeVector[Double] = vector.asBreeze
  }

  implicit class BreezeVectorConversion(val vector: BreezeVector[Double]) extends AnyVal {
    def ml: MLVector = Vectors.fromBreeze(vector)
  }
  
  implicit class MLLibMatrixConversion(val matrix: MLMatrix) extends AnyVal {
    def br: BreezeMatrix[Double] = matrix.asBreeze
  }

  implicit class BreezeMatrixConversion(val matrix: BreezeMatrix[Double]) extends AnyVal {
    def ml: MLMatrix = Matrices.fromBreeze(matrix)
  }

  import  ml.dmlc.xgboost4j.LabeledPoint

  implicit class CSCMatrixFunctions[Int](val csc: CSCMatrix[Int]) extends AnyVal {

//    override def colIter: Iterator[Vector] = {
//
//        Iterator.tabulate(csc.cols) { j =>
//          val colStart = csc.colPtrs(j)
//          val colEnd = csc.colPtrs(j + 1)
//          val ii = csc.rowIndices.slice(colStart, colEnd)
//          val vv = csc.data.slice(colStart, colEnd)
//
//          new SparseVector(csc.rows, ii, vv)
//        }
//
//    }


  }

}