package org.tmoerman.brassica.cases

import java.io.File

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql._
import org.tmoerman.brassica._

import scala.io.Source

/**
  * @author Thomas Moerman
  */
trait DataReader {

  /**
    * The StructField for data Vectors.
    */
  val FEATURES_STRUCT_FIELD = new AttributeGroup(EXPRESSION_VECTOR).toStructField()

  /**
    * Convenience implicit conversion String -> File.
    *
    * @param path The file path as a String.
    * @return Returns java.io.File(path)
    */
  implicit def pimp(path: String): File = new File(path)

  /**
    * @param file
    * @return Returns the list of transcription factors.
    */
  def readTFs(file: String): List[Gene] = Source.fromFile(file).getLines.toList

}