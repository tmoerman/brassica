package org.tmoerman.brassica.cases.megacell

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object MegacellPipeline {

  /**
    * @param spark The SparkSession.
    * @param hdf5 The hdf5 file path, needed for reading the ordered list of genes.
    * @param parquet The path of the parquet file containing (gene, expression) tuples.
    * @param candidateRegulators The Set of candidate regulator genes.
    * @param targets The Set of target genes for which to compute the predictors.
    *                If empty Set is specified, all genes will be considered as targets.
    * @param params The XGBoost regression params.
    * @param cellTop Optional limit for nr of cells to consider in the regressions.
    * @param nrPartitions Optional technical parameter
    * @return
    */
  def apply(spark: SparkSession,
            hdf5: Path,
            parquet: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None) = {

    import spark.implicits._

    val allGenes = MegacellReader.readGeneNames(hdf5).get
    val expressionByGene = spark.read.parquet(parquet).as[ExpressionByGene]

    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      MegacellReader.toCSCMatrix(expressionByGene, globalRegulatorIndex)

    ScenicPipeline
      .apply(
        spark,
        cscProducer,
        expressionByGene,
        allGenes,
        candidateRegulators,
        targets,
        params,
        cellTop,
        nrPartitions)
  }

}