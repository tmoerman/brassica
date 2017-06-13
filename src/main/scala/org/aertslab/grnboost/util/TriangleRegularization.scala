package org.aertslab.grnboost.util

import breeze.linalg.{DenseVector, _}
import breeze.numerics._
import breeze.numerics.constants.Pi

import scala.collection.immutable.Stream.{continually, fill}

/**
  * @author Thomas Moerman
  */
object TriangleRegularization {

  val DEFAULT_PRECISION = 0.001

  type V = DenseVector[Float]

  /**
    * @param gains
    * @param precision
    * @return Returns the inflection point where consecutive values form a straight line with the last value.
    */
  def inflectionPointIndex(gains: List[Float], precision: Double = DEFAULT_PRECISION): Option[Int] = gains match {
    case Nil => None
    case _ :: Nil => Some(1)
    case _ :: _ :: Nil => Some(2)
    case _ :: _ :: _ :: Nil => Some(3)
    case _ =>
      val min = gains.min
      val max = gains.max
      val minMaxScaled = gains.map(g => (g - min) / (max - min))

      val c = DenseVector(gains.length.toFloat - 1, minMaxScaled.last)

      minMaxScaled
        .sliding(2, 1)
        .zipWithIndex
        .toStream
        .dropWhile{
          case (va :: vb :: _, i) =>
            val a = DenseVector(i.toFloat,      va)
            val b = DenseVector(i.toFloat + 1f, vb)
            val theta = angle(a, b, c)

            // println(s"a: $a, b: $b, c: $c, theta: $theta")

            ! ~=(theta, Pi, precision)

          case _ => true
        }
        .headOption
        .map(_._2)
        .orElse(Some(gains.size))
    }

  /**
    * @param gains
    * @param precision
    * @return Returns a Seq of inclusion labels (1=in, 0=out) from the inflection point.
    */
  def labels(gains: List[Float], precision: Double = DEFAULT_PRECISION): Seq[Int] =
    labelStream(inflectionPointIndex(gains, precision))
      .take(gains.size)

  /**
    * @param inflectionPointIndex
    * @return Returns an infinite Stream of inclusion labels (1=in, 0=out) from the inflection point.
    */
  private def labelStream(inflectionPointIndex: Option[Int]): Stream[Int] =
    inflectionPointIndex
      .map(fill(_)(1) ++ continually(0))
      .getOrElse(continually(0))

  private def ~=(x: Double, y: Double, precision: Double) = (x - y).abs < precision

  /**
    * @return Returns the radian angle between three Gain vectors.
    */
  def angle(a: V, b: V, c: V): Double = {
    val x = a - b
    val y = c - b

    val cos_theta = (x dot y) / (norm(x) * norm(y))

    val theta = acos(cos_theta)

    theta
  }

}