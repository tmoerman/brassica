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
    * @param geneNames The list of gene names.
    * @param targetGeneIndex The index of the target gene.
    * @param candidateRegulatorIndices The indices of the candidate regulators.
    * @param boosterParams The XGBoost parameters for this effort.
    * @param nrRounds The nr of training rounds.
    * @param normalize Divide the importances by the sum of importances.
    * @param nrWorkers Technical parallelism parameter.
    * @return Returns DataFrame representing the sub-GRN.
    */
  def apply(spark: SparkSession,
            trainingData: DataFrame,
            geneNames: List[Gene],
            targetGeneIndex: Int,
            candidateRegulatorIndices: Seq[Int],
            boosterParams: BoosterParams,
            nrRounds: Int,
            normalize: Boolean = false,
            nrWorkers: Option[Int] = None): DataFrame = {

    val cleanCandidateRegulatorIndices = candidateRegulatorIndices.filter(_ != targetGeneIndex)

    val sliced: DataFrame = sliceGenes(trainingData, targetGeneIndex, cleanCandidateRegulatorIndices)

    val model =
      SparkXGBoost
        .trainWithDataFrame(
          sliced,
          boosterParams,
          round = nrRounds,
          nWorkers = nrWorkers.getOrElse(spark.sparkContext.defaultParallelism),
          useExternalMemory = false,
          featureCol = CANDIDATE_REGULATORS,
          labelCol = TARGET_GENE_INDEX)

    val sum = model.booster.getFeatureScore().map(_._2.toInt).sum

    val geneRegulations =
      model
        .booster
        .getFeatureScore()
        .map{ case (featureIndex, importance) => {
          val candidateRegulatorIndex = cleanCandidateRegulatorIndices(featureIndex.substring(1).toInt)
          val candidateRegulatorName  = geneNames.apply(candidateRegulatorIndex)
          val targetGeneName = geneNames.apply(targetGeneIndex)
          val finalImportance = if (normalize) (importance.toFloat / sum) else importance.toFloat

          (candidateRegulatorIndex, candidateRegulatorName, targetGeneIndex, targetGeneName, finalImportance) }}
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