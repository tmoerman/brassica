package org.tmoerman.brassica.cases

import org.apache.spark.sql._

/**
  * @author Thomas Moerman
  */
trait DataReader {

  /**
    * Abstract Reader interface to pass to a pipeline.
    *
    * @param spark The SparkSession.
    * @param file The data file.
    * @return Returns a tuple:
    *         - DataFrame of the cell data. By convention, the first
    *         - List of genes.
    */
  def apply(spark: SparkSession, file: String): (DataFrame, List[String])

}