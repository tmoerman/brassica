package org.tmoerman.brassica

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.SparkConf
import org.scalatest.Suite

/**
  * Trait extending H. Karau's DataFrameSuiteBase, with conf overridden for XGBoost.
  *
  * @author Thomas Moerman
  */
trait XGBoostSuiteBase extends DataFrameSuiteBase { self: Suite =>

  override def conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("XGBoost-Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .registerKryoClasses(Array(classOf[Booster]))

}