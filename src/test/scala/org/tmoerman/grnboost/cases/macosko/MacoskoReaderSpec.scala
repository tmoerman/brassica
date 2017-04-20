package org.tmoerman.grnboost.cases.macosko

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.grnboost.GRNBoostSuiteBase
import org.tmoerman.grnboost.cases.DataReader
import org.tmoerman.grnboost.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class MacoskoReaderSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val MACOSKO_NR_GENES = 12953

  val macoskoSampled = props("macoskoSampled")
  val macoskoFull    = props("macoskoFull")

  "reading the sampled Macosko expression set" should "work" in {
    val ds = DataReader.readTxt(spark, macoskoSampled)

    // ds.first.values.size shouldBe ???
    ds.count shouldBe MACOSKO_NR_GENES
  }

  "reading the full Macosko expression set" should "work" in {
    val ds = DataReader.readTxt(spark, macoskoFull)

    // ds.count shouldBe MACOSKO_NR_GENES
  }

}