package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.{XGBoost => SparkXGBoost}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @author Thomas Moerman
  */
object XGBoostRun {

  val TARGET_GENE          = "target"
  val CANDIDATE_REGULATORS = "regulators"
  val CANDIDATE_REGULATOR  = "regulator"
  val IMPORTANCE           = "importance"

  val schema = StructType(Seq(
    StructField(CANDIDATE_REGULATOR, IntegerType, nullable = false),
    StructField(TARGET_GENE,         IntegerType, nullable = false),
    StructField(IMPORTANCE,          DoubleType,  nullable = false)))

  /**
    *
    * @param trainingData
    * @param targetGene
    * @param candidateRegulator
    * @param params
    * @return Returns a DataFrame with (candidateRegulator, target, connectionValue) columns.
    */
  def apply(trainingData: DataFrame,
            targetGene: Int,
            candidateRegulator: Seq[Int],
            params: XGBoostParams,
            nrRounds: Int,
            nrWorkers: Int): DataFrame = {

    val sliced: DataFrame = sliceGenes(trainingData, targetGene, candidateRegulator)

    sliced.show(3)

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

    val score = model.booster.getFeatureScore()

    println(s"score ${score.mkString(", ")}")

    ???
  }

  /**
    * Slice the gene expression vector in function of specified target and candidate regulator gene indices.
    *
    * @param trainingData
    * @param targetGene
    * @param candidateRegulator
    * @return Returns a DataFrame with two attribute groups: "regulators" and "target",
    *         to be used as input to XGBoost.
    */
  def sliceGenes(trainingData: DataFrame,
                 targetGene: Int,
                 candidateRegulator: Seq[Int]): DataFrame = {

    // remove target gene from candidate regulators
    val cleanPredictors = candidateRegulator.filter(_ != targetGene).toArray

    val targetSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION_VECTOR)
        .setOutputCol(TARGET_GENE)
        .setIndices(Array(targetGene))

    val predictorSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION_VECTOR)
        .setOutputCol(CANDIDATE_REGULATORS)
        .setIndices(cleanPredictors)

    val toScalar = udf[Double, MLVector](_.apply(0))

    Some(trainingData)
      .map(targetSlicer.transform)
      .map(predictorSlicer.transform)
      .map(df => df.withColumn(TARGET_GENE, toScalar(df.apply(TARGET_GENE))))
      .map(df => df.select(CANDIDATE_REGULATORS, TARGET_GENE))
      .get
  }

}