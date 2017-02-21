package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.TrackerConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.util.TimeUtils.profile

import scala.concurrent.duration._

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

  val DEFAULT_PARAMS: XGBoostParams = Map(
    "tracker_conf" -> TrackerConf(Duration(0L, MILLISECONDS), "scala"),
    "silent" -> 1

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
    *                            in which case all genes are considered as candidate regulators.
    * @param params The XGBoost parameter Map.
    * @param targets Optional limit for the nr of targets for which to compute regulators. Uses all genes if Nil.
    */
  def apply(spark: SparkSession,
            expressionData: DataFrame,
            genes: List[Gene],
            nrRounds: Int,
            candidateRegulators: List[Gene] = Nil,
            params: XGBoostParams = DEFAULT_PARAMS,
            targets: List[Gene] = Nil) = {
    
    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators)

    type ACC = (List[DataFrame], List[Duration])

    val isTarget = targets match {
      case Nil => (_: Gene) => true
      case _   => targets.toSet.contains _
    }

    val (regulations, timings) =
      genes
        .zipWithIndex
        .filter{ case (gene, _) => isTarget(gene) }
        .map { case (gene, targetIndex) => profile {
          XGBoostRegression(spark, expressionData, targetIndex, candidateRegulatorIndices, params, nrRounds)
        }}
        .foldLeft((Nil, Nil): ACC) { case (acc, (reg, dur)) => (reg :: acc._1, dur :: acc._2) }

    val grn = regulations.reduce(_ union _)

    val total    = timings.reduce(_ plus _)
    val average  = total / timings.length
    val estimate = average * genes.length

    val stats =
      Map(
        "total"    -> total.toUnit(SECONDS),
        "average"  -> average.toUnit(SECONDS),
        "estimate" -> estimate.toUnit(SECONDS))

    (grn, stats)
  }

  /**
    * @param allGenes The List of all genes in the data set.
    * @param candidateRegulators The Set of
    * @return Returns the indices of the subset of genes in the DataFrame,
    *         that also occur in the specified Set of transcription factors.
    */
  def regulatorIndices(allGenes: List[Gene], candidateRegulators: List[Gene]): List[Int] = candidateRegulators match {
    case Nil =>
      allGenes.indices.toList
    case _ =>
      allGenes
        .zipWithIndex
        .filter { case (gene, _) => candidateRegulators.toSet.contains(gene) }
        .map(_._2)
  }

}