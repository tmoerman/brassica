package org.aertslab.grnboost.cases.macosko

import org.aertslab.grnboost.Specs.Server
import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.{DataReader, GRNBoostSuiteBase}
import org.aertslab.grnboost.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class MacoskoReaderSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val MACOSKO_NR_GENES = 12953

  val macoskoSampled = props("macoskoSampled")
  val macoskoFull    = props("macoskoFull")

  "reading the sampled Macosko expression set" should "work" taggedAs Server in {
    val ds = DataReader.readExpressionsByGene(spark, macoskoSampled)

    // ds.first.values.size shouldBe ???
    ds.count shouldBe MACOSKO_NR_GENES
  }

  "reading the full Macosko expression set" should "work" taggedAs Server in {
    val ds = DataReader.readExpressionsByGene(spark, macoskoFull)

    // ds.count shouldBe MACOSKO_NR_GENES
  }

}