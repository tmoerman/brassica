package org.tmoerman.brassica


import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.ScenicPipeline_OLD._

/**
  * @author Thomas Moerman
  */
class ScenicPipelineSpec extends FlatSpec with Matchers {

  behavior of "regulatorIndices"

  val allGenes = List("brca1", "brca2", "hox")

  it should "return indices of all genes when candidates are Nil" in {
    an [AssertionError] shouldBe thrownBy { toGlobalRegulatorIndex(allGenes, Nil) }
  }

  it should "return the correct indices for specified candidates" in {
    toGlobalRegulatorIndex(allGenes, List("brca1", "hox")).map(_._2) shouldBe (0, 2)
  }

}