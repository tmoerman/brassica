package org.tmoerman.brassica.cases.zeisel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader._

/**
  * @author Thomas Moerman
  */
class ZeiselReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "ZeiselReader"

  it should "parse the mouse TFs properly" in {
    val TFs = readTFs(mouseTFs)

    TFs.size shouldBe MOUSE_TF_COUNT
  }

  it should "read column vectors correctly" in {
    import spark.implicits._

    val ds = ZeiselReader.apply(spark, zeiselMrna)

    ds.filter($"gene" === "Dlx1").show()

    ds.head.values.size shouldBe ZEISEL_CELL_COUNT

    ds.count shouldBe ZEISEL_GENE_COUNT
  }

}