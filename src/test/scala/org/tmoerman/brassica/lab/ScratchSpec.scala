package org.tmoerman.brassica.lab

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by tmo on 14/02/17.
  */
class ScratchSpec extends FlatSpec with Matchers {

  "a set" should "behave like a fn" in {

    Set("a", "b").apply("a")

  }

}