package org.tmoerman.brassica

import java.util.Arrays

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.XGBoostSparkRegression.sliceGenes

/**
  * @author Thomas Moerman
  */
class XGBoostSparkRegressionSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  "sliceGenes" should "work with both sparse and dense vectors" in {
    val data = Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -1.1), (1, 1.1)))),
      Row(Vectors.sparse(3, Seq((1, -2.2), (2, 22.22)))),

      Row(Vectors.dense(-1.1, 1.1, 11.11)),
      Row(Vectors.dense(-2.2, 2.2, 22.22))
    )

    val features = new AttributeGroup(EXPRESSION).toStructField()

    val schema = StructType(features :: Nil)

    val df = spark.createDataFrame(data, schema)

    val sliced = sliceGenes(df, 0, Seq(1, 2))

    sliced
      .select("target")
      .collect
      .foreach(_.apply(0).isInstanceOf[Double] shouldBe true)

    sliced
      .select("regulators")
      .collect
      .foreach(_.apply(0).asInstanceOf[MLVector].size shouldBe 2)

    sliced.show()
  }

}