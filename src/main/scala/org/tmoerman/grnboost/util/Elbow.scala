package org.tmoerman.grnboost.util

/**
  * @author Thomas Moerman
  */
object Elbow {

  def apply(x: Array[Double], y: Array[Double], sensitivity: Double = 1d, elbow: Boolean = true) =
    new JKneedle(sensitivity)
      .detectKneeOrElbowPoints(x, y.reverse, elbow)
      .toList
      .map(i => y.size -i -1)
      .reverse

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