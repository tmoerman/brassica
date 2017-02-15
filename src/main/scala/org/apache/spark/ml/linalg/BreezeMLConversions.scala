package org.apache.spark.ml.linalg

import breeze.linalg.{Matrix => BreezeMatrix, Vector => BreezeVector}
import org.apache.spark.ml.linalg.{Vector => MLVector, Matrix => MLMatrix}

/**
  * @author Thomas Moerman
  */
object BreezeMLConversions {

  implicit class MLLibVectorConversion(val vector: MLVector) extends AnyVal {
    def toBreeze: BreezeVector[Double] = vector.asBreeze
  }

  implicit class BreezeVectorConversion(val vector: BreezeVector[Double]) extends AnyVal {
    def toMLLib: MLVector = Vectors.fromBreeze(vector)
  }

  implicit class MLLibMatrixConversion(val matrix: MLMatrix) extends AnyVal {
    def toBreeze: BreezeMatrix[Double] = matrix.asBreeze
  }

  implicit class BreezeMatrixConversion(val matrix: BreezeMatrix[Double]) extends AnyVal {
    def toMLLib: MLMatrix = Matrices.fromBreeze(matrix)
  }

}