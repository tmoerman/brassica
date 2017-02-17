package org.tmoerman.brassica.cases

import java.io.File

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql._
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
trait DataReader {

  /**
    * Abstract Reader interface to pass to a pipeline.
    *
    * @param spark The SparkSession.
    * @param files The data files.
    * @return Returns a tuple:
    *         - DataFrame of the cell data. By convention, the first
    *         - List of genes.
    */
  def apply(spark: SparkSession, files: String*): (DataFrame, List[String])

  /**
    * The StructField for data Vectors.
    */
  val FEATURES_STRUCT_FIELD = new AttributeGroup(EXPRESSION_VECTOR).toStructField()

  /**
    * Convenience implicit conversion String -> File.
    *
    * @param path
    * @return Returns java.io.File(path)
    */
  implicit def pimp(path: String): File = new File(path)

}