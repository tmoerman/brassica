package org.tmoerman.grnboost.cases

import java.io.File

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.util.RDDFunctions._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
object DataReader {

  /**
    * @param spark The SparkSession instance.
    * @param path The file path.
    * @return Returns a Dataset of ExpressionByGene read from the specified path.
    */
  def readTxt(spark: SparkSession, path: Path, delimiter: String = "\t"): Dataset[ExpressionByGene] = {
    import spark.implicits._

    val lines =
      spark
        .sparkContext
        .textFile(path)
        .drop(1)
        .map(_.split(delimiter).map(_.trim).toList)

    val expressionByGene =
      lines
        .map{
          case gene :: values =>

            val length = values.length
            val tuples =
              values
                .zipWithIndex
                .filterNot(_._1 == "0")
                .map{ case (e, i) => (i, e.toDouble) }

            ExpressionByGene(gene, Vectors.sparse(length, tuples))

          case _ => ???
        }
        .toDS

    expressionByGene
  }

  /**
    * @param spark The SparkSession.
    * @param ds The Dataset of ExpressionByGene instances.
    * @return Returns the List of genes.
    */
  def toGenes(spark: SparkSession, ds: Dataset[ExpressionByGene]): List[Gene] = {
    import spark.implicits._

    ds.select($"gene").rdd.map(_.getString(0)).collect.toList
  }

  /**
    * Convenience implicit conversion String -> File.
    *
    * @param path The file path as a String.
    * @return Returns java.io.File(path)
    */
  implicit def pimpPath(path: String): File = new File(path)

  /**
    * @param file
    * @return Returns the list of transcription factors.
    */
  def readTFs(file: String) = Source.fromFile(file).getLines.toList

}