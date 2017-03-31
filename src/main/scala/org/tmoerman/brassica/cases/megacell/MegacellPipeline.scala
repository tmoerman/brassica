package org.tmoerman.brassica.cases.megacell

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
@deprecated
object MegacellPipeline {

  /**
    * @param spark The SparkSession.
    * @param hdf5 The hdf5 file path, needed for reading the ordered list of genes.
    * @param parquet The path of the parquet file containing (gene, expression) tuples.
    * @param candidateRegulators The Set of candidate regulator genes.
    * @param targets The Set of target genes for which to compute the predictors.
    *                If empty Set is specified, all genes will be considered as targets.
    * @param params The XGBoost regression params.
    * @param nrPartitions Optional technical parameter
    * @return Returns a Dataset of Regulation instances.
    */
  def apply(spark: SparkSession,
            hdf5: Path,
            parquet: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: XGBoostRegressionParams = XGBoostRegressionParams(),
            nrPartitions: Option[Int] = None) = {

    import spark.implicits._

    val expressionByGene = spark.read.parquet(parquet).as[ExpressionByGene]

    // TODO any preprocessing on the Dataset should be done here, downstream agnostic of nr cells under consideration.

    ScenicPipeline
      .computeRegulations(
        expressionByGene,
        candidateRegulators,
        targets,
        params,
        nrPartitions)
  }

}