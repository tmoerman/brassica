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

  /**
    * @param values The list of values in which to find the inflection point.
    * @param precision The precision of the radian angle.
    * @return Returns the inflection point and its index where consecutive values form a straight line with the last value.
    *
    *         The inflection point is the first point that makes an angle equal to PI (in radians) with its
    *         successor and the last point in the data set.
    */
  def inflectionPointIndex(values: List[Float],
                           precision: Double = DEFAULT_PRECISION,
                           xScale: Int = 10): Option[Int] =
    values match {
      case Nil => None
      case _ :: Nil => None
      case _ :: _ :: Nil => None
      case _ :: _ :: _ :: Nil => None
      case _ =>
        val min = values.min
        val max = values.max
        val minMaxScaled = values.map(g => (g - min) / (max - min))
        
        val c = DenseVector(values.length.toFloat * xScale, minMaxScaled.last)

        minMaxScaled
          .sliding(2, 1)
          .zipWithIndex
          .toStream
          .dropWhile {
            case (va :: vb :: _, i) =>
              val a = DenseVector(i.toFloat,      va)
              val b = DenseVector(i.toFloat + 1f, vb)
              val theta = angle(a, b, c)

              ! ~=(theta, Pi, precision)

            case _ => ??? // a.k.a. ka-boom
          }
          .headOption
          .map{ case (_, idx) => idx }
    }

  /**
    * @param list
    * @param precision
    * @return Returns a Seq of inclusion labels (1=in, 0=out) from the inflection point.
    */
  def labels(list: List[Float], precision: Double = DEFAULT_PRECISION): Seq[Int] =
    labelStream(inflectionPointIndex(list, precision))
      .take(list.size)

  /**
    * @param inflectionPointIndex
    * @return Returns an infinite Stream of inclusion labels (1=in, 0=out) from the inflection point.
    */
  private def labelStream(inflectionPointIndex: Option[Int]): Stream[Int] =
    inflectionPointIndex
      .map(fill(_)(1) ++ continually(0))
      .getOrElse(continually(1))

  private def ~=(x: Double, y: Double, precision: Double) = (x - y).abs < precision

  type V = DenseVector[Float]

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