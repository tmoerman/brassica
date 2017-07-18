package org.aertslab.grnboost.cases.megacell

import java.io.File

import ch.systemsx.cisd.hdf5.HDF5FactoryProvider
import gov.llnl.spark.hdf.reader.{HDF5Reader, HDF5Schema}
import org.aertslab.grnboost.Specs.Server
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

/**
  * @author Thomas Moerman
  */
class ParseSpec extends FlatSpec with Matchers {

  val path = "/Users/tmo/work/ghb2016/data/bigsc/1M_neurons_filtered_gene_bc_matrices_h5.h5"

  val bigsc = new File(path)

  behavior of "parsing a hdf5 file"

  it should "work" taggedAs Server ignore {
    val reader = HDF5FactoryProvider.get().openForReading(bigsc)

    val members = reader.getGroupMembers("/mm10").asScala

    reader.int32.readArrayBlockWithOffset("/mm10/data", 10, 20)

    println(members)
  }

  it should "print node info" taggedAs Server ignore {
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
      case _ => ???
    }

    println(node)
  }

}