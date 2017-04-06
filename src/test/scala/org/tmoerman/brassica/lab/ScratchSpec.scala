package org.tmoerman.brassica.lab

import java.util.Calendar

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by tmo on 14/02/17.
  */
class ScratchSpec extends FlatSpec with Matchers {

  "a set" should "behave like a fn" in {
    Set("a", "b").apply("a")
  }

  "meh" should "meh" in {
    val bla = Calendar.getInstance.getTime.formatted("yyyy.MM.dd.hh")
    println(bla)
  }


  "sliding window evaluation" should "be lazy" in {
    val r =
      (0 until 10)
        .toStream
        .map(i => {println(i); i})
        .sliding(3, 1)
        .takeWhile(_.last <= 77)

    println(r.mkString(" -- "))

  }

}