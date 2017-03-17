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
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None) = {

    val allGenes = readGeneNames(hdf5).get

    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      readCSCMatrix(
        hdf5,
        cellTop,
        Some(globalRegulatorIndex.map(_._2)))
      .get

    val columnVectorRDD = spark.read.parquet(columnsParquet).rdd

    ScenicPipeline
      .apply(
        spark,
        cscProducer,
        columnVectorRDD,
        allGenes,
        candidateRegulators,
        targets,
        params,
        cellTop,
        nrPartitions)
  }

}