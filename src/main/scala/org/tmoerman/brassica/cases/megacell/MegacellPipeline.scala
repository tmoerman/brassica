package org.tmoerman.brassica.cases.megacell

import org.apache.spark.sql.{Row, SparkSession}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.megacell.MegacellReader._

/**
  * @author Thomas Moerman
  */
object MegacellPipeline {

  /**
    * @param spark
    * @param hdf5
    * @param columnsParquet
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param cellTop
    */
  def apply(spark: SparkSession,
            // TODO rows parquet
            hdf5: Path,
            columnsParquet: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None) = {

    val allGenes = MegacellReader.readGeneNames(hdf5).get

    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      MegacellReader
        .readCSCMatrix(
          hdf5,
          cellTop,
          Some(globalRegulatorIndex.map(_._2)))
        .get

    val expressionByGene = spark.read.parquet(columnsParquet)

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