package org.tmoerman.brassica.util

/**
  * @author Thomas Moerman
  */
object Elbow {

  /**
    * http://stackoverflow.com/questions/2018178/finding-the-best-trade-off-point-on-a-curve
    */
  def findIn(s: Seq[Int]): Int = {
    val sorted = s.sorted.zipWithIndex

    val min = sorted.head
    val max = sorted.last

    ???
  }

}