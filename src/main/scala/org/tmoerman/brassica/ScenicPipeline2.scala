package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.{XGBoost => ScalaXGBoost}
import org.apache.spark.sql.SparkSession

/**
  * @author Thomas Moerman
  */
object ScenicPipeline2 {

  def apply(spark: SparkSession,
            rawPath: String,
            parquetPath: String,
            candidateRegulators: List[Gene] = Nil,
            targets: List[Gene] = Nil,
            xgBoostParams: XGBoostParams = XGBoostParams()) = {

//    val denseExpressionMatrix = expressionMatrix.toDense
//
//    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators)
//
//    val regulatorExpressionMatrix = denseExpressionMatrix(::, candidateRegulatorIndices)
//
//    val broadcast = spark.sparkContext.broadcast(regulatorExpressionMatrix)
//
//    val targetResponseVectors = denseExpressionMatrix(*, ::).map(v => v.toString())

    // ScalaXGBoost.train()

    // TODO slice and parallelize the columns of the expressionMatrix ifo targets

    // TODO slice and broadcast the regulator submatrix ifo candidateRegulators

    // TODO perform regressions in parallel

    // TODO reduce result into a GRN

    // TODO DMatrix instances should be disposed !!!!

  }

}
