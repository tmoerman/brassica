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
    * @param spark The SparkSession.
    * @param file The data file path.
    * @param dataReader The DataReader suitable for parsing the specified file.
    * @param candidateRegulators The list of candidate regulators (transcription factors) or Nil,
    *                             in which case all genes are considered as candidate regulators.
    * @param params The XGBoost parameter Map.
    * @param nrRounds The nr of rounds XGBoost parameter.
    */
  def apply(spark: SparkSession,
            file: String,
            dataReader: DataReader,
            candidateRegulators: List[Gene] = Nil,
            params: XGBoostParams = DEFAULT_PARAMS,
            nrWorkers: Int,
            nrRounds: Int) = {

    val (df, genes) = dataReader(spark, file)

    val candidateRegulatorIndices = regulatorIndices(genes, candidateRegulators.toSet)

    val GRN =
       genes
        .zipWithIndex
        .map { case (_, targetIndex) =>
          XGBoostRegression(spark, df, targetIndex, candidateRegulatorIndices, params, nrRounds, nrWorkers) }
        .reduce(_ union _)

    GRN
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