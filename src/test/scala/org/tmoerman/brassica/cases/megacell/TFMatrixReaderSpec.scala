package org.tmoerman.brassica.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.ScenicPipeline._

import org.apache.spark.ml.linalg.BreezeMLConversions._

/**
  * @author Thomas Moerman
  */
class TFMatrixReaderSpec extends FlatSpec with Matchers {

  "reading the TF matrix" should "work" in {
    val limit = Some(10)

    val (nrCells, _) = MegacellReader.readDimensions(megacell).get

    val genes = MegacellReader.readGeneNames(megacell).get

    val TFs = MegacellReader.readTFs(mouseTFs)

    val candidateRegulatorIndices = regulatorIndexMap(genes, TFs).values.toSeq

    val (csc, _) =
      MegacellReader
        .readCSCMatrix(megacell, cellTop = limit, onlyGeneIndices = Some(candidateRegulatorIndices))
        .get

    csc.rows shouldBe limit.getOrElse(nrCells)
    csc.cols shouldBe candidateRegulatorIndices.size

    val dm = MegacellReader.toXGBoostDMatrix(csc)

    dm.rowNum shouldBe limit.getOrElse(nrCells)

    val col13 = csc.columns.drop(12).next

    println(col13)
  }

}