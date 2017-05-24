package org.aertslab.grnboost.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.GRNBoostSuiteBase
import org.aertslab.grnboost.util.PropsReader.props

import org.aertslab.grnboost.cases.DataReader._

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredReaderSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val zeiselFiltered = props("zeiselFiltered")

  "reading the filtered zeisel data" should "work" in {
    val ds = readExpression(spark, zeiselFiltered)

    ds.head.gene shouldBe "Tspan12"

    println(ds.count)

    ds.collect
  }

  "reading the filtered zeisel list of genes" should "work" in {
    val ds = readExpression(spark, zeiselFiltered)

    val top5 = toGenes(spark, ds).take(5)

    top5 shouldBe List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")
  }

}