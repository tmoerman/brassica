package org.tmoerman.brassica.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.ScenicPipeline.regulatorIndices

/**
  * @author Thomas Moerman
  */
class TFMatrixReaderSpec extends FlatSpec with Matchers {

  "reading the TF matrix" should "work" in {
    val limit = Some(10) // TODO test on full!
    
    val genes = MegacellReader.readGeneNames(megacell).get

    val TFs = MegacellReader.readTFs(mouseTFs)

    val candidateRegulatorIndices = regulatorIndices(genes, TFs)

    val pred = Some(candidateRegulatorIndices)

    val csc =
      MegacellReader
        .readCSCMatrix(megacell, cellTop = limit, onlyGeneIndices = pred)
        .get

    csc.rows shouldBe limit.get
    csc.cols shouldBe candidateRegulatorIndices.size

    val dm = MegacellReader.toXGBoostDMatrix(csc)

    dm.rowNum shouldBe limit.get
  }

}