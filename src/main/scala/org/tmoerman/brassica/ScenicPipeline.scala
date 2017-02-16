package org.tmoerman.brassica

import ml.dmlc.xgboost4j.scala.spark.TrackerConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.cases.DataReader

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
    * @param spark
    * @param file
    * @param dataReader
    * @param transcriptionFactors
    * @param params
    * @param nrRounds
    */
  def apply(spark: SparkSession,
            file: String,
            dataReader: DataReader,
            transcriptionFactors: List[Gene] = Nil,
            params: XGBoostParams = DEFAULT_PARAMS,
            nrWorkers: Int,
            nrRounds: Int): DataFrame = {

    val (df, genes) = dataReader(spark, file)

    val candidateRegulators = regulatorIndices(genes, transcriptionFactors.toSet)

    val grn =
       genes
        .zipWithIndex
        .map { case (_, targetIndex) => XGBoostRun(df, targetIndex, candidateRegulators, params, nrRounds, nrWorkers) }
        .reduce(_ union _)

    grn
  }

  /**
    * @param genes
    * @param transcriptionFactors
    * @return Returns the indices of the subset of genes in the DataFrame,
    *         that also occur in the specified Set of transcription factors.
    */
  def regulatorIndices(genes: List[Gene], transcriptionFactors: Set[Gene]): List[Int] =
    genes
      .zipWithIndex
      .filter { case (gene, _) => transcriptionFactors.toSet.contains(gene) }
      .map(_._2)

}