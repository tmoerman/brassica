package org.tmoerman.brassica.lab

import breeze.linalg._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Thomas Moerman
  */
class BreezeLab extends FlatSpec with Matchers {

  "adding two CSC matrices" should "work" in {
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

  "slicing a dense vector" should "work" in {
    val v = Array(1f, 2f, 3f, 4f, 5f)
    val d = new DenseVector[Float](v)
    val s = d.apply(Seq(2, 3))

    println(s.toArray.toList)
  }

  "slicing rows of a CSC matrix" should "work" in {
    val m = new CSCMatrix.Builder[Int](rows = 4, cols = 4)
    m.add(0,0,1)
    m.add(1,1,1)
    m.add(2,2,1)
    m.add(3,3,1)
    val m1 = m.result

    println(m1.toDense)
    println(" --- ")

    val s = m1(Seq(1, 2), 0 until m1.cols)

    println(s.toDenseMatrix)
  }

}
