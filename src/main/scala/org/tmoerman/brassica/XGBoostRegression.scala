package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.{XGBoost => SparkXGBoost}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Thomas Moerman
  */
object XGBoostRegression {

  /**
    * @param spark The SparkSession.
    * @param trainingData The DataFrame to train the regression model with.
    * @param targetGeneIndex The index of the target gene.
    * @param candidateRegulatorsIndices The indices of the candidate regulators.
    * @param params The XGBoost parameter Map.
    * @return Returns a DataFrame with (candidateRegulator, target, connectionValue) columns.
    */
  def apply(spark: SparkSession,
            trainingData: DataFrame,
            genes: List[Gene],
            targetGeneIndex: Int,
            candidateRegulatorsIndices: Seq[Int],
            params: XGBoostParams,
            nrRounds: Int,
            nrWorkers: Option[Int] = None): DataFrame = {

    val cleanCandidateRegulators = candidateRegulatorsIndices.filter(_ != targetGeneIndex)

    val sliced: DataFrame = sliceGenes(trainingData, targetGeneIndex, cleanCandidateRegulators)

    val model =
      SparkXGBoost
        .trainWithDataFrame(
          sliced,
          params,
          round = nrRounds,
          nWorkers = nrWorkers.getOrElse(spark.sparkContext.defaultParallelism),
          useExternalMemory = false,
          featureCol = CANDIDATE_REGULATORS,
          labelCol = TARGET_GENE_INDEX)

    val geneRegulations =
      model
        .booster
        .getFeatureScore()
        .values
        .zip(cleanCandidateRegulators)
        .map{ case (importance, candidateRegulatorIndex) => {
          val targetGeneName = genes.apply(targetGeneIndex)
          val candidateRegulatorName = genes.apply(candidateRegulatorIndex)

          (candidateRegulatorIndex, candidateRegulatorName, targetGeneIndex, targetGeneName, importance.toInt)
        }}
        .toSeq

    spark
      .createDataFrame(geneRegulations)
      .toDF(CANDIDATE_REGULATOR_INDEX, CANDIDATE_REGULATOR_NAME, TARGET_GENE_INDEX, TARGET_GENE_NAME, IMPORTANCE)
      .sort(desc(IMPORTANCE))
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
        .setOutputCol(TARGET_GENE_INDEX)
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
      .map(df => df.withColumn(TARGET_GENE_INDEX, toScalar(df.apply(TARGET_GENE_INDEX))))
      .map(df => df.select(CANDIDATE_REGULATORS, TARGET_GENE_INDEX))
      .get
  }

}