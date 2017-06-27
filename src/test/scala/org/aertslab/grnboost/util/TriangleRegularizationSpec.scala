package org.aertslab.grnboost.util

import java.util.concurrent.atomic.AtomicInteger

import breeze.linalg._
import breeze.numerics.constants._
import org.aertslab.grnboost.util.TriangleRegularization.{angle, inflectionPoint, labels}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class TriangleRegularizationSpec extends FlatSpec with Matchers {

  val gains = List(
    7.07E+14,
    4.81E+13,
    3.65E+13,
    1.98E+14,
    1.78E+14,
    7424315.0,
    7252873.0,
    6299353.0,
    4545308.0,
    1615836.0,
    1442182.0,
    1094521.0,
    1072370.0,
    752762.0,
    623782.0,
    541074.0,
    504379.0,
    421922.0,
    415787.0).map(_.toFloat)

  val convex = List(10f, 9.9f, 9.8f, 9.7f, 0f)

  "angle between vectors in radians" should "work" in {
    val a = DenseVector(0f, 1f)
    val b = DenseVector(1f, 1f)
    val c = DenseVector(20f, 1f)

    angle(a, b, c) shouldBe Pi
  }

  behavior of "finding the inflection point index"

  it should "yield None for an empty input list" in {
    inflectionPoint(Nil) shouldBe None
  }

  it should "yield the inflection point for an example list" in {
    inflectionPoint(gains).map(_._2) shouldBe Some(5)
    labels(gains).take(gains.size)   shouldBe List.fill(5)(1) ++ List.fill(14)(0)
  }

  it should "yield all for a convex list" in {
    inflectionPoint(convex)          shouldBe None
    labels(convex).take(convex.size) shouldBe List.fill(5)(1)
  }

  it should "yield all for a singleton list" in {
    val in = 5f :: Nil

    inflectionPoint(in)      shouldBe None
    labels(in).take(in.size) shouldBe List(1)
  }

  it should "yield all for a pair" in {
    val in = 6f :: 5f :: Nil

    inflectionPoint(in)      shouldBe None
    labels(in).take(in.size) shouldBe List(1, 1)
  }

  it should "yield all for a triplet" in {
    val in = 10f :: 2f :: 1f :: Nil

    inflectionPoint(in)      shouldBe None
    labels(in).take(in.size) shouldBe List(1, 1, 1)
  }

  it should "work on a lazy list of reductions stream" in {
    val atom = new AtomicInteger(0)
    
    val point =
      gains
        .sliding(4, 4)
        .toStream
        .scanLeft(List[Float]()){ (acc, next) => {
          atom.incrementAndGet

          acc ::: next
        } }
        .flatMap(inflectionPoint(_, xScaleFactor = 10))
        .headOption
        .map(_._2)

    atom.get shouldBe 2

    point shouldBe Some(5)
  }

}