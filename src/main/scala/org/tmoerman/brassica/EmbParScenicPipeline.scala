package org.tmoerman.brassica

import breeze.linalg.CSCMatrix
import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica.ScenicPipeline.regulatorIndices

import breeze.linalg._
import breeze.numerics._

/**
  * @author Thomas Moerman
  */
object EmbParScenicPipeline {

  def apply(spark: SparkSession,
            expressionMatrix: CSCMatrix[Int],
            genes: List[Gene],
            candidateRegulators: List[Gene] = Nil,
            targets: List[Gene] = Nil,
            xgBoostParams: XGBoostParams = XGBoostParams()) = {

    val denseExpressionMatrix = expressionMatrix.toDense

    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators)

    val regulatorExpressionMatrix = denseExpressionMatrix(::, candidateRegulatorIndices)

    val broadcast = spark.sparkContext.broadcast(regulatorExpressionMatrix)

    val targetResponseVectors = denseExpressionMatrix(*, ::).map(v => v.toString())


    // TODO slice and parallelize the columns of the expressionMatrix ifo targets

    // TODO slice and broadcast the regulator submatrix ifo candidateRegulators

    // TODO perform regressions in parallel

    // TODO reduce result into a GRN

  }

}
