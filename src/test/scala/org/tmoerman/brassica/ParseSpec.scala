package org.tmoerman.brassica

import java.io.File

import ch.systemsx.cisd.hdf5.HDF5FactoryProvider
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import gov.llnl.spark.hdf.reader.{HDF5Schema, HDF5Reader}
import gov.llnl.spark.hdf.reader.HDF5Schema.{Dataset, HDF5Node}
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConverters._

/**
  * @author Thomas Moerman
  */
class ParseSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  val path = "/Users/tmo/work/ghb2016/data/bigsc/1M_neurons_filtered_gene_bc_matrices_h5.h5"

  val bigsc = new File(path)

  behavior of "parsing a hdf5 file"

  it should "work" in {
    val reader = HDF5FactoryProvider.get().openForReading(bigsc)

    val members = reader.getGroupMembers("/mm10").asScala

    reader.int32.readArrayBlockWithOffset("/mm10/data", 10, 20)

    println(members)
  }

  it should "print node info" in {
    import HDF5Schema._

    val reader = new HDF5Reader(bigsc)

    val node: HDF5Node = reader.listMembers("/mm10/data")

    node match {
      case d: Dataset[_] =>
        val a = d.contains.readArrayBlock(reader.reader, 100, 0)
        val b = d.contains.readArrayBlock(reader.reader, 50, 1)

        val c = reader.reader.int32.readArrayBlockWithOffset("/mm10/data", 10, 3)

        println(a.toList)
        println(b.toList)
        println(c.toList)
    }

    println(node)
  }

  it should "print members overview" in {

    val h5Reader = HDF5FactoryProvider.get().openForReading(bigsc)
    val members = h5Reader.getGroupMembers("/mm10").asScala

    val dfReader = sqlContext.read.format("gov.llnl.spark.hdf")

    val memberList = List("barcodes", "gene_names", "genes", "shape", "indices", "indptr") // , "indices", "data" "indptr"]

    memberList.par

    memberList.foreach(member => {
      val df = dfReader.option("dataset", s"/mm10/$member").load(path)

      println(member)
      df.printSchema
      df.show(10)
    })

  }

  it should "read a dataset in column blocks" in {

    val INDPTR  = "/mm10/indptr"
    val DATA    = "/mm10/data"
    val INDICES = "/mm10/indices"

    val h5Reader = HDF5FactoryProvider.get().openForReading(bigsc)

    val pointers = h5Reader.int64.readArray(INDPTR)


    //TODO zip with index => idx = column index of the CSC matrix!

    val size_offset_tuples =
      pointers
        .sliding(2, 1)
        .toList
        .par
        .map{ case Array(a, b) => {
          val size   = b - a
          val offset = a

          val dataBlock = h5Reader.int16.readArrayBlockWithOffset(DATA,    size.toInt, offset)
          //val idxBlock  = h5Reader.int32.readArrayBlockWithOffset(INDICES, size.toInt, offset)

          val r = dataBlock.max

          r
        }}
        .max

    println(s"max = $size_offset_tuples")

  }

}