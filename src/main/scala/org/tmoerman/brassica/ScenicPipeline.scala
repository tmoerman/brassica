package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.TrackerConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.util.TimeUtils
import org.tmoerman.brassica.util.TimeUtils.time

import scala.concurrent.duration.{Duration, MILLISECONDS}

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

  val DEFAULT_PARAMS: XGBoostParams = Map(
    "tracker_conf" -> TrackerConf(Duration.apply(0L, MILLISECONDS), "scala")
  )

  /**
    * See XGBoost docs:
    *   - https://github.com/dmlc/xgboost/blob/master/doc/parameter.md
    *   - https://github.com/dmlc/xgboost/issues/332
    *
    * @param spark The SparkSession.
    * @param expressionData The DataFrame containing the expression data.
    * @param genes The List of all genes corresponding to the columns in the DataFrame.
    * @param candidateRegulators The list of candidate regulators (transcription factors) or Nil,
    *                             in which case all genes are considered as candidate regulators.
    * @param params The XGBoost parameter Map.
    * @param nrRounds The nr of rounds XGBoost parameter.
    */
  def apply(spark: SparkSession,
            expressionData: DataFrame,
            genes: List[Gene],
            candidateRegulators: List[Gene] = Nil,
            params: XGBoostParams = DEFAULT_PARAMS,
            nrWorkers: Int,
            nrRounds: Int) = {
    
    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators.toSet)

    type ACC = (List[DataFrame], List[Duration])

    val acc: ACC = (Nil, Nil)

    val (regulations, timings) =
      genes
        .zipWithIndex
        .map { case (_, targetIndex) => time {
          XGBoostRegression
            .apply(spark, expressionData, targetIndex, candidateRegulatorIndices, params, nrRounds, nrWorkers) }}
        .foldLeft(acc) { case ((regulations, durations), (df, t)) => (df :: regulations, t :: durations) }

    val grn = regulations.reduce(_ union _)

    (grn, timings)
  }

  /**
    * @param allGenes The List of all genes in the data set.
    * @param candidateRegulators The Set of
    * @return Returns the indices of the subset of genes in the DataFrame,
    *         that also occur in the specified Set of transcription factors.
    */
  def regulatorIndices(allGenes: List[Gene], candidateRegulators: Set[Gene]): List[Int] =
    allGenes
      .zipWithIndex
      .filter { case (gene, _) => candidateRegulators.toSet.contains(gene) }
      .map(_._2)

}