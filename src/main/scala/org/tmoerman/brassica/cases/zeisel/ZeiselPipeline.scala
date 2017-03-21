package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.zeisel.ZeiselReader.toExpressionByGene

/**
  * @author Thomas Moerman
  */
object ZeiselPipeline {

  /**
    * @param spark The SparkSession.
    * @param file The raw zeisel mRNA file path.
    * @param candidateRegulators The set of candidate regulator genes.
    * @param targets The Set of target genes for which to compute the predictors.
    *                If empty Set is specified, all genes will be considered as targets.
    * @param params The XGBoost regression params.
    * @param nrPartitions Optional technical parameter specifying parallelism.
    * @return Returns a Dataset of Regulation instances.
    */
  def apply(spark: SparkSession,
            file: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            nrPartitions: Option[Int] = None) = {

    import spark.implicits._

    val lines = ZeiselReader.rawLines(spark, file)

    val allGenes = ZeiselReader.readGenes(lines)

    val expressionByGene = lines.flatMap(toExpressionByGene).toDS

    ScenicPipeline
      .apply(
        spark,
        expressionByGene,
        allGenes,
        candidateRegulators,
        targets,
        params,
        nrPartitions)
  }

}