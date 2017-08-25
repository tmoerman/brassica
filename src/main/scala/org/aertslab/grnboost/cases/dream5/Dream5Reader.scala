package org.aertslab.grnboost.cases.dream5

import breeze.linalg._
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.{ExpressionByGene, Gene, Path}
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

/**
  * Reader function for the Dream 5 training data.
  *
  * @author Thomas Moerman
  */
object Dream5Reader {

  /**
    * @param spark The SparkSession.
    * @param path The data file.
    * @param tfFile The TF file.
    * @return Returns the expression matrix by gene Dataset and the List of TF genes.
    */
  def readTrainingData(spark: SparkSession,
                       path: Path,
                       tfFile: Path,
                       delimiter: Char = '\t'): (Dataset[ExpressionByGene], List[Gene]) = {

    import spark.implicits._

    val TFs = Source.fromFile(tfFile).getLines.toList

    val genes = Source.fromFile(path).getLines.next.split(delimiter).toSeq

    val matrix = csvread(path.file, delimiter, skipLines = 1)

    val rows = matrix.t.ml.rowIter.map(_.toSparse).toSeq

    val ds =
      (genes zip rows)
        .map{ case (gene, expression) => ExpressionByGene(gene, expression) }
        .toDS
        .cache

    (ds, TFs)
  }

}