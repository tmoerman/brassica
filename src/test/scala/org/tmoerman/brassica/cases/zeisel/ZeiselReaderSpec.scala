package org.tmoerman.brassica.cases.zeisel

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.linalg.SparseVector
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.zeisel.ZeiselReader._

/**
  * @author Thomas Moerman
  */
class ZeiselReaderSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  behavior of "ZeiselReader"

  it should "parse the schema correctly" in {
    val lines = rawLines(spark, zeiselMrna)

    val schema = parseSchema(lines)

    schema.size shouldBe 11

    schema.exists(_.name == "cell_id") shouldBe true

    schema.exists(_.name == "expression") shouldBe true
  }

  val FIRST_FIVE_GENES = List("Tspan12", "Tshz1", "Fnbp1l", "Adamts15", "Cldn12")

  it should "parse the gene names correctly" in {
    val lines = rawLines(spark, zeiselMrna)

    val genes = readGenes(lines)

    genes.take(5) shouldBe FIRST_FIVE_GENES
  }

  it should "parse the gene names quickly" in {
    val genes = readGenes(spark, zeiselMrna)

    genes.take(5) shouldBe FIRST_FIVE_GENES
  }

  it should "parse the DataFrame correctly" in {
    val (df, _) = apply(spark, zeiselMrna)

    df.count shouldBe ZEISEL_CELL_COUNT

    df.show(5)
  }

  it should "parse the mouse TFs properly" in {
    val TFs = readTFs(mouseTFs)

    TFs.size shouldBe MOUSE_TF_COUNT
  }

  it should "read column vectors correctly" in {
    val lines = ZeiselReader.rawLines(spark, zeiselMrna).cache

    val columnsDF = ZeiselReader.readExpressionByGene(spark, lines)

    columnsDF.show(5)

    columnsDF.head.getAs[SparseVector](VALUES).size shouldBe ZEISEL_CELL_COUNT

    columnsDF.count shouldBe ZEISEL_GENE_COUNT
  }

  it should "read CSC matrix (cells * genes)" in {
    val lines = ZeiselReader.rawLines(spark, zeiselMrna)

    val csc = ZeiselReader.readCSCMatrix(
      lines,
      nrCells = ZEISEL_CELL_COUNT,
      nrGenes = ZEISEL_GENE_COUNT)

    csc.cols shouldBe ZEISEL_GENE_COUNT

    csc.rows shouldBe ZEISEL_CELL_COUNT
  }

  behavior of "Zeisel Parquet I/O"

  it should "write the gene expression DF to parquet" in {
    if (! new File(zeiselParquet).exists) {
      val (df, _) = apply(spark, zeiselMrna)

      df.write.parquet(zeiselParquet)
    }
  }

  it should "read the gene expression DF from .parquet" in {
    val (df, genes) = fromParquet(spark, zeiselParquet, zeiselMrna)

    df.show(5)

    df.count shouldBe ZEISEL_CELL_COUNT

    genes.size shouldBe ZEISEL_GENE_COUNT
  }

  it should "convert the DF to CSCMatrix" in {
    val (df, genes) = fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs = readTFs(mouseTFs).toSet

    val globalRegulatorIndex = ScenicPipeline.toGlobalRegulatorIndex(genes, TFs)

    val csc = toCSCMatrix(df, onlyGeneIndices = globalRegulatorIndex.map(_._2))

    csc.rows shouldBe ZEISEL_CELL_COUNT

    csc.cols shouldBe globalRegulatorIndex.size
  }

}