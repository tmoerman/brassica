package org.tmoerman.brassica

import java.util.Arrays

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.XGBoostRun.sliceGenes

/**
  * @author Thomas Moerman
  */
class XGBoostRunSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  "slice genes" should "work" in {
    val data = Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -1.1), (1, 1.1)))),
      Row(Vectors.sparse(3, Seq((1, -2.2), (2, 22.22)))),

      Row(Vectors.dense(-1.1, 1.1, 11.11)),
      Row(Vectors.dense(-2.2, 2.2, 22.22))
    )

    val features = new AttributeGroup(EXPRESSION_VECTOR).toStructField()

    val schema = StructType(features :: Nil)

    val df = spark.createDataFrame(data, schema)

    val sliced = sliceGenes(df, 0, Seq(1, 2))

    sliced.show()
  }

}