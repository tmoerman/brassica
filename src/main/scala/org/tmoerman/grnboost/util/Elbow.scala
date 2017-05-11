package org.tmoerman.grnboost.util

/**
  * @author Thomas Moerman
  */
object Elbow {

  def apply(y: Seq[Double], sensitivity: Double = 1d) = {
    val x = (0 until y.size)

    new JKneedle(sensitivity)
      .detectElbowPoints(
        x.map(_.toDouble).toArray,
        y.reverse.toArray)
      .toList
      .map(i => x.last -i -1)
      .reverse
  }

//  type Index = Int
//
//  def detectKneeOrElbow(x: Seq[Double], y: Seq[Double], elbow: Boolean = true): Seq[Index] = {
//    assert(x.size == y.size)
//    assert(x.size > 1)
//
//
//  }
//
//  def normalize(values: Seq[Double]): Seq[Double] = {
//    val min = values.min
//    val max = values.max
//    values.map(v => (v - min) / (max - min))
//  }

}