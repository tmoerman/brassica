package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica.{CellCount, Gene, Path, RegressionParams}

/**
  * @author Thomas Moerman
  */
object ZeiselPipeline {

  def apply(spark: SparkSession,
            // TODO
            hdf5: Path,
            columnsParquet: Path,
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            paramms: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None) = {

    ???
  }

}