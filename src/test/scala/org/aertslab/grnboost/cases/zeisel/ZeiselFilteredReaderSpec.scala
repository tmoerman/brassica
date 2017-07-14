package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.GRNBoostSuiteBase
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.Specs.Server

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredReaderSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val zeiselFiltered = props("zeiselFiltered")

  "reading the filtered zeisel data" should "work" taggedAs Server in {
    val ds = readExpressionsByGene(spark, zeiselFiltered)

    ds.head.gene shouldBe "Tspan12"

    println(ds.count)

    ds.collect
  }

  "reading the filtered zeisel list of genes" should "work" taggedAs Server in {
    val ds = readExpressionsByGene(spark, zeiselFiltered)

    val top5 = toGenes(spark, ds).take(5)

    top5 shouldBe List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")
  }

}