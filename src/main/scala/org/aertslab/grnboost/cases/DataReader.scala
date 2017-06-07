package org.aertslab.grnboost.cases

import java.io.File

import org.aertslab.grnboost._
import org.aertslab.grnboost.util.RDDFunctions._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
object DataReader {

  /**
    * @param spark The SparkSession instance.
    * @param path The file path.
    * @param nrHeaders The number of header lines in the file.
    * @return Returns a Dataset of ExpressionByGene read from the specified path.
    */
  def readExpressionsByGene(spark: SparkSession,
                            path: Path,
                            nrHeaders: Int = 1,
                            delimiter: String = "\t"): Dataset[ExpressionByGene] = {

    import spark.implicits._

    spark
      .sparkContext
      .textFile(path)
      .drop(nrHeaders)
      .map(_.split(delimiter).map(_.trim).toList)
      .map{
        case gene :: values =>

          val length = values.length
          val tuples =
            values
              .zipWithIndex
              .map{ case (v, idx) => (idx, v.toDouble) }
              .filterNot{ case (_, v) => v == 0d }

          ExpressionByGene(gene, Vectors.sparse(length, tuples))

        case _ => ???
      }
      .toDS
  }

  /**
    * @param spark The SparkSession.
    * @param ds The Dataset of ExpressionByGene instances.
    * @return Returns the List of genes.
    */
  def toGenes(spark: SparkSession, ds: Dataset[ExpressionByGene]): List[Gene] = {

    import spark.implicits._

    ds
      .select($"gene")
      .rdd
      .map(_.getString(0))
      .collect
      .toList
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
  def readRegulators(file: String) = Source.fromFile(file).getLines.map(_.trim).filterNot(_.isEmpty).toList

}