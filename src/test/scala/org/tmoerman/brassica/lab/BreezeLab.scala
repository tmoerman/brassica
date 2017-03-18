package org.tmoerman.brassica.lab

import breeze.linalg.CSCMatrix
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by tmo on 18/03/17.
  */
class BreezeLab extends FlatSpec with Matchers {

  "adding two sparse matrices" should "work" in {
    val b1 = new CSCMatrix.Builder[Int](rows = 4, cols = 4)
    b1.add(0,0,1)
    b1.add(0,1,1)
    b1.add(0,2,1)
    b1.add(0,3,1)
    val m1 = b1.result

    val b2 = new CSCMatrix.Builder[Int](rows = 4, cols = 4)
    b2.add(3,0,7)
    b2.add(3,1,7)
    b2.add(3,2,7)
    b2.add(3,3,7)
    val m2 = b2.result

    val m3 = m1 + m2

    println(m1.toDense)
    println
    println(m2.toDense)
    println
    println(m3.toDense)
  }

}
