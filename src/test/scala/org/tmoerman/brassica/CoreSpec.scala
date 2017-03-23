package org.tmoerman.brassica

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class CoreSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  import spark.implicits._

  behavior of "Dataset of ExpressionByGene"

  it should "slice" in {
    val ds =
      Seq(
        ExpressionByGene("Dlx1", Vectors.sparse(5, Seq((1, 1d), (3, 3d)))),
        ExpressionByGene("Dlx2", Vectors.sparse(5, Seq((2, 2d), (4, 4d)))))
      .toDS
    
    val sliced = ds.slice(Seq(0, 1))

    sliced.head.values.toDense.toArray shouldBe Array(0d, 1d)
  }

}