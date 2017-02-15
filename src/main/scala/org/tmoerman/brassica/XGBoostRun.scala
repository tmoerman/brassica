package org.tmoerman.brassica

import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.DataFrame

/**
  * @author Thomas Moerman
  */
object XGBoostRun {

  val TARGET_GENE          = "target"
  val CANDIDATE_REGULATORS = "regulators"

  def apply(trainingData: DataFrame,
            targetGene: Int,
            candidateRegulator: Seq[Int],
            xGBoostParams: XGBoostParams): DataFrame = {

    val sliced: DataFrame = sliceGenes(trainingData, targetGene, candidateRegulator)

    sliced.show(2)

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

    Some(trainingData)
      .map(targetSlicer.transform)
      .map(predictorSlicer.transform)
      .map(_.select(CANDIDATE_REGULATORS, TARGET_GENE))
      .get
  }

}