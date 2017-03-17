package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.{XGBoost => XGBoostSpark}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Thomas Moerman
  */
object XGBoostSparkRegression {

  /**
    * @param spark The SparkSession.
    * @param trainingData The DataFrame to train the regression model with.
    * @param geneNames The list of gene names.
    * @param targetGeneIndex The index of the target gene.
    * @param regulatorIndices The indices of the regulators.
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
            regulatorIndices: Seq[Int],
            boosterParams: BoosterParams,
            nrRounds: Int,
            normalize: Boolean = false,
            nrWorkers: Option[Int] = None): DataFrame = {

    // TODO slicer in an ML transformer

    val cleanRegulatorIndices = regulatorIndices.filter(_ != targetGeneIndex)

    val sliced: DataFrame = sliceGenes(trainingData, targetGeneIndex, cleanRegulatorIndices)

    val model =
      XGBoostSpark
        .trainWithDataFrame(
          sliced,
          boosterParams,
          round = nrRounds,
          nWorkers = nrWorkers.getOrElse(spark.sparkContext.defaultParallelism),
          useExternalMemory = false,
          featureCol = REGULATORS,
          labelCol = TARGET_GENE)

    val sum = model.booster.getFeatureScore().map(_._2.toInt).sum

    val geneRegulations =
      model
        .booster
        .getFeatureScore()
        .map{ case (featureIndex, importance) => {
          val regulatorIndex = cleanRegulatorIndices(featureIndex.substring(1).toInt)
          val regulatorName  = geneNames.apply(regulatorIndex)
          val targetGeneName = geneNames.apply(targetGeneIndex)
          val normalizedImportance = if (normalize) importance.toFloat / sum else importance.toFloat

          (regulatorIndex, regulatorName, targetGeneIndex, targetGeneName, normalizedImportance) }}
        .toSeq

    spark
      .createDataFrame(geneRegulations)
      .toDF(REGULATOR_INDEX, REGULATOR_NAME, TARGET_INDEX, TARGET_NAME, IMPORTANCE)
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
  def sliceGenes(trainingData: DataFrame, targetGene: GeneIndex, candidateRegulators: Seq[GeneIndex]): DataFrame = {
    val targetSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION)
        .setOutputCol(TARGET_GENE)
        .setIndices(Array(targetGene))

    val predictorSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION)
        .setOutputCol(REGULATORS)
        .setIndices(candidateRegulators.toArray)

    val toScalar = udf[Double, MLVector](_.apply(0)) // transform vector of length 1 to scalar.

    Some(trainingData)
      .map(targetSlicer.transform)
      .map(predictorSlicer.transform)
      .map(df => df.withColumn(TARGET_GENE, toScalar(df.apply(TARGET_GENE))))
      .map(df => df.select(REGULATORS, TARGET_GENE))
      .get
  }

}