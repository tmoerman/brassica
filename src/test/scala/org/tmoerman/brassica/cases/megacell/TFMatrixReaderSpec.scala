package org.tmoerman.brassica.cases.megacell

import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.ScenicPipeline_OLD._

/**
  * @author Thomas Moerman
  */
class TFMatrixReaderSpec extends FlatSpec with Matchers {

  "reading the TF matrix" should "work" in {
    val limit = Some(10)

    val (nrCells, _) = MegacellReader.readDimensions(megacell).get

    val genes = MegacellReader.readGeneNames(megacell).get

    val TFs = MegacellReader.readTFs(mouseTFs)

    val candidateRegulatorIndices = toGlobalRegulatorIndex(genes, TFs).map(_._2)

    val csc =
      MegacellReader
        .readCSCMatrix(megacell, cellTop = limit, onlyGeneIndices = Some(candidateRegulatorIndices))
        .get

    csc.rows shouldBe limit.getOrElse(nrCells)
    csc.cols shouldBe candidateRegulatorIndices.size

//    val bla = csc(0 until limit.get, (0 until csc.cols).take(20))
//    val dense = bla.toDenseMatrix
//    val ddm = MegacellReader.toXGBoostDMatrix(dense)

    val dm = MegacellReader.toDMatrix(csc)

    dm.rowNum shouldBe limit.getOrElse(nrCells)

    val col13 = csc.columns.drop(12).next

    println(col13)
  }

}