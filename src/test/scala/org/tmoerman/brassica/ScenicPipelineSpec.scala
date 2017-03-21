package org.tmoerman.brassica

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader
import org.tmoerman.brassica.cases.zeisel.ZeiselReader.ZEISEL_CELL_COUNT
import org.tmoerman.brassica.util.PropsReader

/**
  * @author Thomas Moerman
  */
class ScenicPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  val zeiselMrna = PropsReader.props("zeisel")

  "converting to a CSCMatrix" should "work" in {
    val ds = ZeiselReader.apply(spark, zeiselMrna)

    val predictors = "Tspan12" :: Nil

    val csc = ScenicPipeline.toRegulatorCSCMatrix(ds, predictors)

    csc.rows shouldBe ZEISEL_CELL_COUNT
    csc.cols shouldBe 1

    println(csc.toDense.t)
  }

}