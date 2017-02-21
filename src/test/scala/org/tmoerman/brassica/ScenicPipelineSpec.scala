package org.tmoerman.brassica


import org.scalatest.{Matchers, FlatSpec}

import ScenicPipeline._

/**
  * @author Thomas Moerman
  */
class ScenicPipelineSpec extends FlatSpec with Matchers {

  behavior of "regulatorIndices"

  val allGenes = List("brca1", "brca2", "hox")

  it should "return indices of all genes when candidates are Nil" in {
    regulatorIndices(allGenes, Nil) shouldBe (0 until 3)
  }

  it should "return the correct indices for specified candidates" in {
    regulatorIndices(allGenes, List("brca1", "hox")) shouldBe List(0, 2)
  }

}