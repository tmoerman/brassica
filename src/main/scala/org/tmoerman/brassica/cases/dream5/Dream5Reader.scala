package org.tmoerman.brassica.cases.dream5

import breeze.linalg._
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.tmoerman.brassica.Gene
import org.tmoerman.brassica.cases.DataReader

import scala.collection.JavaConversions._
import scala.io.Source.fromFile

/**
  * @author Thomas Moerman
  */
object Dream5Reader extends DataReader {

  /**
    * @param file
    * @return Returns the List of TFs from the specified file
    */
  def TFs(file: String) = fromFile(file).getLines().toList

  /**
    * Abstract Reader interface to pass to a pipeline.
    *
    * @param spark The SparkSession.
    * @param files The data files for Dream5 in order:
    *              1. {species}_data.tsv
    *              2. {species}_gene_names.tsv
    * @return Returns a tuple:
    *         - DataFrame of the cell data. By convention, the first
    *         - List of genes.
    */
  def apply(spark: SparkSession, files: String*): (DataFrame, List[Gene]) =
    (files: @unchecked) match { case Seq(dataFile, genesFile, _ @ _*) =>
      val data = csvread(dataFile, '\t').t.ml

      val rows = data.rowIter.map(Row(_)).toList

      val genes = fromFile(genesFile).getLines().toList

      val schema = StructType(FEATURES_STRUCT_FIELD :: Nil)

      val df = spark.createDataFrame(rows, schema)

      (df, genes)
    }

}
