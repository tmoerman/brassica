package org.tmoerman.brassica.lab

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.Regulation

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

  it should "roll up a Dataset" in {
    import spark.implicits._

    import org.apache.spark.sql.functions._

    val dream1 =
      spark
        .sparkContext
        .textFile("/media/tmo/data/work/datasets/dream5/out/Network1/part-00000")
        .map(_.split("\t"))
        .map{ case Array(reg, tar, imp) => Regulation(reg, tar, imp.toFloat) }
        .toDS()

    val sums = dream1.rollup("target").agg(stddev("importance"), sum("importance"), max("importance"))

    sums.show()

    // sums.describe("sum(importance)").show


  }

}

case class KV(key: String, value: Int)