package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Dataset, SparkSession}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.DataReader
import org.tmoerman.brassica.util.RDDFunctions._

/**
  * @author Thomas Moerman
  */
object ZeiselFilteredReader extends DataReader{

  private[zeisel] val ZEISEL_FILTERED_COUNT = 13063

  /**
    * @param spark
    * @param path
    * @return
    */
  def apply(spark: SparkSession, path: Path): Dataset[ExpressionByGene] = {
    import spark.implicits._

    val lines =
      spark
        .sparkContext
        .textFile(path)
        .drop(1)
        .map(_.split("\t").map(_.trim).toList)

    val expressionByGene =
      lines
        .map{
          case gene :: rest =>

            val length = rest.length
            val tuples = rest.map(_.toDouble).zipWithIndex.filterNot(_._1 == 0).map{ case (e, i) => (i, e) }
            ExpressionByGene(gene, Vectors.sparse(length, tuples))

          case _ => ???
        }
        .toDS

    expressionByGene
  }

  /**
    * @param spark The SparkSession.
    * @param ds The Dataset of ExpressionByGene instances.
    * @return Returns the List of genes.
    */
  def toGenes(spark: SparkSession, ds: Dataset[ExpressionByGene]): List[Gene] = {
    import spark.implicits._

    ds.select($"gene").rdd.map(_.getString(0)).collect.toList
  }

}