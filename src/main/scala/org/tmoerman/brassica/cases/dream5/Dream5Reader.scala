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
  * Reader function for the Dream 5 training data.
  *
  * @author Thomas Moerman
  */
object Dream5Reader extends DataReader {

  /**
    * @param spark The SparkSession.
    * @param dataFile The .tsv containing the expression data.
    * @return Returns the expression matrix DataFrame and the List of genes.
    */
  def readTrainingData(spark: SparkSession, dataFile: String): (DataFrame, List[Gene]) = {
    val expressionMatrix = csvread(dataFile, '\t', skipLines = 1).t

    val df = toDF(spark, expressionMatrix)

    val genes = fromFile(dataFile).getLines.next.split('\t').map(_.trim).toList

    (df, genes)
  }

  /**
    * @param spark The SparkSession.
    * @param dataFile The .tsv containing the expression data.
    * @param genesFile The .tsv containing the gene names.
    * @return Returns the expression matrix DataFrame and the List of genes.
    */
  def readOriginalData(spark: SparkSession, dataFile: String, genesFile: String): (DataFrame, List[Gene]) = {
    val expressionMatrix = csvread(dataFile, '\t').t

    val df = toDF(spark, expressionMatrix)

    val genes = fromFile(genesFile).getLines.toList

    (df, genes)
  }

  private[this] def toDF(spark: SparkSession, data: DenseMatrix[Double]): DataFrame = {
    val rows = data.ml.rowIter.map(Row(_)).toList

    val schema = StructType(FEATURES_STRUCT_FIELD :: Nil)

    spark.createDataFrame(rows, schema)
  }

}