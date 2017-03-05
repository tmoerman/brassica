package org.tmoerman.brassica.cases.megacell

import breeze.linalg.{CSCMatrix, SparseVector, DenseMatrix => BDM}
import ch.systemsx.cisd.hdf5.{HDF5FactoryProvider, IHDF5Reader}
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.ml.linalg.{SparseMatrix, DenseMatrix}
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
    * @return
    */
  @deprecated def apply(spark: SparkSession, path: String): Try[(DataFrame, List[Gene])] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(reader => {

        println("reading CSC matrix")
        val csc = readCSCMatrix(reader, None)

        println("converting to DF")
        //val df    = toDataFrame(spark, csc)

        println("reader genes")
        val genes = readGeneNames(reader)

        (???, genes)
      }).tried

  /**
    * @param path The file path.
    * @param limit Optional limit on how many cells to read from the file - for testing purposes.
    * @return Returns a  CSCMatrix of Ints.
    */
  def readCSCMatrix(path: String, limit: Option[Int] = None): Try[CSCMatrix[Int]] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map{ reader => readCSCMatrix(reader, limit) }
      .tried

  /**
    * @param reader The managed HDF5 Reader instance.
    * @param cellLimit Optional limit on how many cells to read from the file - for testing purposes.
    * @return Returns a CSCMatrix of Ints.
    */
  def readCSCMatrix(reader: IHDF5Reader,
                    cellLimit: Option[Int]): CSCMatrix[Int] = {

    val (nrGenes, nrCells) = readDimensions(reader)

    val pointers = reader.int64.readArray(INDPTR)

    val blocks = pointers.sliding(2, 1).zipWithIndex.toList

    assert(blocks.size == nrCells)

    val matrixBuilder = new CSCMatrix.Builder[Int](rows = nrCells, cols = nrGenes)

    type GeneIndex = Int
    type ExpressionValue = Int

    def readBlock(reader: IHDF5Reader, offset: Long, next: Long): (Array[GeneIndex], Array[ExpressionValue]) = {
      val blockSize = (next - offset).toInt

      val geneIndices = reader.int32.readArrayBlockWithOffset(INDICES, blockSize, offset)

      val expressionValues = reader.int32.readArrayBlockWithOffset(DATA, blockSize, offset)

      (geneIndices, expressionValues)
    }

    cellLimit
      .map(blocks.take)
      .getOrElse(blocks)
      .foreach{ case (Array(offset, next), cellIndex) => {
        val (geneIndices, expressionValues) = readBlock(reader, offset, next)

        for (i <- geneIndices.indices) matrixBuilder.add(cellIndex, geneIndices(i), expressionValues(i))
      }}

    matrixBuilder.result
  }

  def readSparseVectors(path: String, limit: Option[Int]): Try[List[SparseVector[Double]]] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(reader => readSparseVectors(reader, limit))
      .tried

  def readSparseVectors(reader: IHDF5Reader, limit: Option[Int]): List[SparseVector[Double]] = {
    val (nrGenes, nrCells) = readDimensions(reader)

    val pointers = reader.int64.readArray(INDPTR)

    val blocks =
      pointers
        .sliding(2, 1)
        .zipWithIndex
        .toList

    assert(blocks.size == nrCells)

    val sparseVectors =
      blocks
        .take(limit.getOrElse(nrCells))
        .map{ case (Array(offset, next), cellIndex) => {

          val blockSize = (next - offset).toInt

          val expressionValues = reader.int32.readArrayBlockWithOffset(DATA,    blockSize, offset).map(_.toDouble)
          val geneIndices = reader.int32.readArrayBlockWithOffset(INDICES, blockSize, offset)
          val tuples = geneIndices zip expressionValues

          val vector = breeze.linalg.SparseVector.apply(nrGenes)(tuples: _*)

          vector
        }}

    sparseVectors //.map(Row(_))
  }

  /**
    * @param path The file path.
    * @return Returns tuple (nrGenes, nrCells).
    */
  def readDimensions(path: String): Try[(Int, Int)] =
    managed(HDF5FactoryProvider.get.openForReading(path))
      .map(readDimensions)
      .tried

  /**
    * @param reader The managed HDF5 reader instance.
    * @return Returns tuple (nrGenes, nrCells)
    */
  def readDimensions(reader: IHDF5Reader): (Int, Int) = reader.int32.readArray(SHAPE) match {
    case Array(nrGenes, nrCells) => (nrGenes, nrCells)
    case _ => throw new Exception("Could not read shape.")
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