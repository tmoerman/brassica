package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.{XGBoost => SparkXGBoost}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @author Thomas Moerman
  */
object XGBoostRegression {

  val TARGET_GENE          = "target"
  val CANDIDATE_REGULATORS = "regulators"
  val CANDIDATE_REGULATOR  = "regulator"
  val IMPORTANCE           = "importance"

  /**
    * @param spark The SparkSession.
    * @param trainingData The DataFrame to train the regression model with.
    * @param targetGene The index of the target gene.
    * @param candidateRegulators The indices of the candidate regulators.
    * @param params The XGBoost parameter Map.
    * @return Returns a DataFrame with (candidateRegulator, target, connectionValue) columns.
    */
  def apply(spark: SparkSession,
            trainingData: DataFrame,
            targetGene: Int,
            candidateRegulators: Seq[Int],
            params: XGBoostParams,
            nrRounds: Int,
            nrWorkers: Int): DataFrame = {

    val cleanCandidateRegulators = candidateRegulators.filter(_ != targetGene)

    val sliced: DataFrame = sliceGenes(trainingData, targetGene, cleanCandidateRegulators)

    val model =
      SparkXGBoost
        .trainWithDataFrame(
          sliced,
          params,
          round = nrRounds,
          nWorkers = nrWorkers,
          useExternalMemory = false, // TODO try with false
          featureCol = CANDIDATE_REGULATORS,
          labelCol = TARGET_GENE)

    val regulations =
      model
        .booster
        .getFeatureScore()
        .values
        .zip(cleanCandidateRegulators)
        .map{ case (importance, candidateRegulator) => (candidateRegulator, targetGene, importance.toInt) }
        .toSeq

    spark
      .createDataFrame(regulations)
      .toDF(CANDIDATE_REGULATOR, TARGET_GENE, IMPORTANCE)
  }

  /**
    * Slice the gene expression vector in function of specified target and candidate regulator gene indices.
    *
    * @param trainingData The DataFrame to slice.
    * @param targetGene The index of the target gene.
    * @param candidateRegulators The indices of the candidate regulators.
    * @return Returns a DataFrame with two attribute groups: "regulators" and "target",
    *         to be used as input to XGBoost.
    */
  def sliceGenes(trainingData: DataFrame, targetGene: Int, candidateRegulators: Seq[Int]): DataFrame = {
    val targetSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION_VECTOR)
        .setOutputCol(TARGET_GENE)
        .setIndices(Array(targetGene))

    val predictorSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION_VECTOR)
        .setOutputCol(CANDIDATE_REGULATORS)
        .setIndices(candidateRegulators.toArray)

    val toScalar = udf[Double, MLVector](_.apply(0)) // transform vector of length 1 to scalar.

    Some(trainingData)
      .map(targetSlicer.transform)
      .map(predictorSlicer.transform)
      .map(df => df.withColumn(TARGET_GENE, toScalar(df.apply(TARGET_GENE))))
      .map(df => df.select(CANDIDATE_REGULATORS, TARGET_GENE))
      .get
  }

}