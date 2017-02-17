package org.tmoerman.brassica.lab

import java.util.Arrays

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._

/**
  * Created by tmo on 14/02/17.
  */
class VectorSlicerSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  "it" should "illustrate how Vector slicing works" in {

    val data = List(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3")) // [TMO] you can specify either indices or column names to slice.
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)

  }

}