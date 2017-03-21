package org.tmoerman.brassica.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.XGBoostSuiteBase
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredReaderSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  val zeiselFiltered = props("zeiselFiltered")

  "reading the filtered zeisel data" should "work" in {
    val ds = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    ds.head.gene shouldBe "Tspan12"

    println(ds.count)

    ds.collect
  }

  "reading the filtered zeisel list of genes" should "work" in {
    val ds = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    val top5 = ZeiselFilteredReader.toGenes(spark, ds).take(5)

    top5 shouldBe List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")
  }

}