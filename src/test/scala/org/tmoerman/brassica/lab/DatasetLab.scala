package org.tmoerman.brassica.lab

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class DatasetLab extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "Dataset"

  it should "filter by set" in {
    import spark.implicits._

    val ds = List(KV("a", 1), KV("b", 2), KV("c", 3)).toDS()

    val pred = Set("c")

    val filtererd = ds.filter(kv => pred.contains(kv.key))

    filtererd.show()
  }

}

case class KV(key: String, value: Int)