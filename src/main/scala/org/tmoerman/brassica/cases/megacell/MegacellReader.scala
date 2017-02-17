package org.tmoerman.brassica.cases.megacell

import java.io.File

import breeze.linalg.CSCMatrix
import breeze.linalg.CSCMatrix._
import ch.systemsx.cisd.hdf5.HDF5FactoryProvider
import resource._

import scala.util.Try

/**
  * @author Thomas Moerman
  */
object MegacellReader {

  // TODO override DataReader

  private[this] val INDPTR  = "/mm10/indptr"
  private[this] val DATA    = "/mm10/data"
  private[this] val INDICES = "/mm10/indices"
  private[this] val SHAPE   = "/mm10/shape"

  /**
    * Reads the CSC matrix from the Chromium Megacell file.
    *
    * Source:
    *  -> https://support.10xgenomics.com/single-cell/datasets/1M_neurons
    *  -> aggr - Gene / cell matrix HDF5 (filtered)	3.79 GB	c134b01776f4e7da81a24c0b0a45c4b6
    *
    * @param path Path of the h5 file with CSC data structure.
    * @param n The number of columns to actually read from the h5 file (only use for testing).
    */
  def apply(path: String, n: Option[Int] = None): Try[CSCMatrix[Int]] =
    managed(HDF5FactoryProvider.get.openForReading(new File(path)))
      .map(reader => {

        val (rows, cols) = reader.int32.readArray(SHAPE) match {
          case Array(a, b) => (a, b)
          case _ => throw new Exception("Could not read shape.")
        }

        // TODO val genes = reader.string.readArray()

        val pointers = reader.int64.readArray(INDPTR)

        val blocks =
          pointers
            .sliding(2, 1)
            .zipWithIndex
            .take(n.getOrElse(cols))
            .toList

        val matrix =
          blocks
            .par
            .map{ case (Array(offset, next), col) => {
              val size  = next - offset

              val values     = reader.int32.readArrayBlockWithOffset(DATA,    size.toInt, offset)
              val rowIndices = reader.int32.readArrayBlockWithOffset(INDICES, size.toInt, offset)

              (values zip rowIndices)
                .foldLeft(zeros[Int](rows, cols)){
                  case (acc, (v, row)) => acc.update(row, col, v); acc }
            }}
            .reduce(_ += _)

        matrix
      }).tried

}
