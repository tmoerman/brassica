package org.tmoerman.brassica

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.util.TimeUtils.{pretty, profile}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

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
            candidateRegulators: List[Gene],
            params: BoosterParams = DEFAULT_BOOSTER_PARAMS,
            targets: List[Gene] = Nil,
            nrWorkers: Option[Int] = None) = {
    
    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators)

    type ACC = (List[DataFrame], List[Duration])

    val isTarget = targets match {
      case Nil => (_: Gene) => true
      case _   => targets.toSet.contains _
    }

    val repartitioned = expressionData.repartition(nrWorkers.getOrElse(spark.sparkContext.defaultParallelism)).cache()

    val (regulations, timings) =
      genes
        .zipWithIndex
        .filter{ case (gene, _) => isTarget(gene) }
        .map { case (targetGene, targetIndex) => profile {
          XGBoostSparkRegression(
            spark,
            repartitioned,
            genes,
            targetIndex,
            candidateRegulatorIndices,
            params,
            nrRounds = nrRounds,
            nrWorkers = nrWorkers) }}
        .foldLeft((Nil, Nil): ACC) { case (acc, (reg, dur)) => (reg :: acc._1, dur :: acc._2) }

    val grn = regulations.reduce(_ union _)

    val total    = timings.reduce(_ plus _)
    val average  = total / timings.length
    val estimate = average * genes.length

    val stats =
      ListMap(
        "nr of cells"           -> expressionData.count,
        "nr of genes"           -> genes.size,
        "nr of target genes"    -> targets.size,
        "nr of regulator genes" -> s"${candidateRegulatorIndices.size} (${candidateRegulators.size} specified)",

        "nr of rounds" -> nrRounds,
        "nr of workers" -> nrWorkers.map(_.toString).getOrElse(s"default parallelism ${spark.sparkContext.defaultParallelism}"),

        "edge count" -> grn.count,

        s"total time on ${targets.size} targets"       -> pretty(total),
        "average time on 1 target"                     -> pretty(average),
        s"estimated time on all ${genes.size} targets" -> pretty(estimate)
      )

    (grn, stats ++ params)
  }

  /**
    * @param genes The List of all genes in the data set.
    * @param candidateRegulators The Set of
    * @return Returns the indices of the subset of genes in the DataFrame,
    *         that also occur in the specified Set of transcription factors.
    */
  def regulatorIndices(genes: List[Gene], candidateRegulators: List[Gene]): List[Int] = candidateRegulators match {
    case Nil =>
      genes.indices.toList
    case _ =>
      genes
        .zipWithIndex
        .filter { case (gene, _) => candidateRegulators.toSet.contains(gene) }
        .map(_._2)
  }

}