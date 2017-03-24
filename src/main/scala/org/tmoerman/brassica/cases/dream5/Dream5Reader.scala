package org.tmoerman.brassica.cases.dream5

import breeze.linalg._
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.tmoerman.brassica.{ExpressionByGene, Gene}
import org.tmoerman.brassica.cases.DataReader

import scala.io.Source

/**
  * Reader function for the Dream 5 training data.
  *
  * @author Thomas Moerman
  */
object Dream5Reader extends DataReader {

  val TAB = '\t'

  /**
    * @param spark The SparkSession.
    * @param dataFile The data file.
    * @param tfFile The TF file.
    * @return Returns the expression matrix by gene Dataset and the List of TFs.
    */
  def readTrainingData(spark: SparkSession,
                       dataFile: String,
                       tfFile: String): (Dataset[ExpressionByGene], List[Gene]) = {

    import spark.implicits._

    val TFs = Source.fromFile(tfFile).getLines.toList

    val genes = Source.fromFile(dataFile).getLines.next.split(TAB)
    val matrix = csvread(dataFile, TAB, skipLines = 1)
    val ds =
      (genes.toSeq zip matrix.t.ml.rowIter.toSeq)
        .map{ case (gene, expression) => ExpressionByGene(gene, expression) }
        .toDS

    (ds, TFs)
  }

}