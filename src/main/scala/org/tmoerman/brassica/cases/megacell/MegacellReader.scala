package org.tmoerman.brassica.cases.megacell

import breeze.linalg.CSCMatrix
import breeze.linalg.CSCMatrix._
import ch.systemsx.cisd.hdf5.{IHDF5Reader, HDF5FactoryProvider}
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.tmoerman.brassica.Gene
import org.tmoerman.brassica.cases.DataReader
import resource._

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Reads the CSC matrix from the Chromium Megacell file.
  *
  * Source:
  *  -> https://support.10xgenomics.com/single-cell/datasets/1M_neurons
  *  -> aggr - Gene / cell matrix HDF5 (filtered)	3.79 GB	c134b01776f4e7da81a24c0b0a45c4b6
  *
  * @author Thomas Moerman
  */
object MegacellReader extends DataReader {

  // TODO override DataReader

  private[this] val INDPTR     = "/mm10/indptr"
  private[this] val DATA       = "/mm10/data"
  private[this] val INDICES    = "/mm10/indices"
  private[this] val SHAPE      = "/mm10/shape"
  private[this] val GENE_NAMES = "/mm10/gene_names"

  /**
    * @param spark The SparkSession.
    * @param path Path of the h5 file with CSC data structure.
    *
    * @return
    */
  def apply(spark: SparkSession, path: String): Try[(DataFrame, List[Gene])] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(reader => {

        println("reading CSC matrix")
        val csc   = readCSCMatrix(reader)

        println("converting to DF")
        val df    = toDataFrame(spark, csc)

        println("reader genes")
        val genes = readGeneNames(reader)

        (df, genes)
      }).tried

  def readCSCMatrix(path: String): Try[CSCMatrix[Double]] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(readCSCMatrix)
      .tried

  def readCSCMatrix(reader: IHDF5Reader): CSCMatrix[Double] = {
    val (rows, cols) = reader.int32.readArray(SHAPE) match {
      case Array(a, b) => (a, b)
      case _ => throw new Exception("Could not read shape.")
    }

    val pointers = reader.int64.readArray(INDPTR)

    val blocks =
      pointers
        .sliding(2, 1)
        .zipWithIndex
        //.take(n.getOrElse(cols)) // TODO meh...
        .toList

    val matrix =
      blocks
        .par // TODO parallel necessary ???
        .map{ case (Array(offset, next), col) => {
          val size  = next - offset

          val values     = reader.int32.readArrayBlockWithOffset(DATA,    size.toInt, offset).map(_.toDouble)
          val rowIndices = reader.int32.readArrayBlockWithOffset(INDICES, size.toInt, offset)

          (values zip rowIndices)
            .foldLeft(zeros[Double](rows, cols)){
              case (acc, (v, row)) => acc.update(row, col, v); acc }
          }}
        .reduce(_ += _)
        .t // transpose

    matrix
  }

  def readGeneNames(path: String): Try[List[Gene]] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(readGeneNames)
      .tried

  def readGeneNames(reader: IHDF5Reader) = reader.string.readArray(GENE_NAMES).toList

  def toDataFrame(spark: SparkSession, csc: CSCMatrix[Double]): DataFrame = {
    val rows = csc.ml.rowIter.map(v => Row(v)).toList

    val schema = StructType(FEATURES_STRUCT_FIELD :: Nil)

    spark.createDataFrame(rows, schema)
  }

}