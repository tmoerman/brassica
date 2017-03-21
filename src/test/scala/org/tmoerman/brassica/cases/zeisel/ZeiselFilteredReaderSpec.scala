package org.tmoerman.brassica.cases.zeisel

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline, XGBoostSuiteBase}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader.ZEISEL_CELL_COUNT
import org.tmoerman.brassica.util.PropsReader

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredReaderSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  val path = PropsReader

  "reading the filtered zeisel data" should "work" in {
    val ds = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    ds.head.gene shouldBe "Tspan12"

    println(ds.count)

    ds.collect
  }

  "converting to a CSCMatrix" should "work" in {
    val ds = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    val predictors = "Tspan12" :: Nil

    val csc = ScenicPipeline.toRegulatorCSCMatrix(ds, predictors)

    csc.rows shouldBe ZEISEL_CELL_COUNT
    csc.cols shouldBe 1

    println(csc.toDense.t)
  }

  "reading the filtered zeisel list of genes" should "work" in {
    val ds = ZeiselFilteredReader.apply(spark, zeiselFiltered)

    val top5 = ZeiselFilteredReader.toGenes(spark, ds).take(5)

    top5 shouldBe List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")
  }

}