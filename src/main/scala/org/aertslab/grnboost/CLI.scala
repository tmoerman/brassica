package org.aertslab.grnboost

import java.io.File

import scopt.OptionParser
import com.softwaremill.quicklens._
import scopt.RenderingMode.OneColumn

import scala.util.Try

/**
  * Command line argument parser.
  * @author Thomas Moerman
  */
object CLI extends OptionParser[Config]("GRNBoost") {

  private val input =
    opt[File]("input").abbr("i")
      .required
      .valueName("<file|dir>")
      .validate(file => if (file.exists) success else failure(s"Input file ($file) does not exist."))
      .text(
        """
          |  REQUIRED. Input file or directory.
        """.stripMargin)
      .action{ case (file, cfg) => cfg.modify(_.inf.each.input).setTo(Some(file)) }

  private val skipHeaders =
    opt[Int]("skip-headers").abbr("skip")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  The number of input file header lines to skip. Default: 0.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.skipHeaders).setTo(nr) }

  private val output =
    opt[File]("output").abbr("o")
      .required
      .valueName("<dir>")
      .validate(file => if (file.exists) failure(s"output file ($file) already exists") else success)
      .text(
        """
          |  REQUIRED. Output directory.
        """.stripMargin)
      .action{ case (file, cfg) => cfg.modify(_.inf.each.output).setTo(Some(file)) }

  private val regulators =
    opt[File]("regulators").abbr("tf")
      .required
      .valueName("<file>")
      .validate(file => if (file.exists) success else failure(s"Regulators file ($file) does not exist."))
      .text(
        """
          |  REQUIRED. Text file containing the regulators (transcription factors), one regulator per line.
        """.stripMargin)
      .action{ case (file, cfg) => cfg.modify(_.inf.each.regulators).setTo(Some(file)) }

  private val delimiter =
    opt[String]("delimiter").abbr("d")
      .optional
      .valueName("<del>")
      .text(
        """
          |  The delimiter to use in input and output files. Default: TAB.
        """.stripMargin)
      .action{ case (del, cfg) => cfg.modify(_.inf.each.delimiter).setTo(del) }

  private val outputFormat =
    opt[String]("output-format").abbr("of")
      .optional
      .valueName("<list|matrix|parquet>")
      .validate(string =>
        Try(Format(string))
          .map(_ => success)
          .getOrElse(failure(s"unknown output format (${string.toLowerCase}")))
      .text(
        """
          |  Output format. Default: list.
        """.stripMargin)
      .action{ case (format, cfg) => cfg.modify(_.inf.each.outputFormat).setTo(Format.apply(format)) }

  private val sample =
    opt[Double]("sample").abbr("s")
      .optional
      .valueName("<nr>")
      .validate(pct => if (pct <= 0f || pct > 1f) failure("sample-pct must be > 0.0 && <= 1.0.") else success)
      .text(
        """
          |  Use a sample of size <nr> of the observations to infer the GRN.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.sampleSize.each).setTo(nr.toInt) }

  private val targets =
    opt[Seq[Gene]]("targets")
      .optional
      .valueName("<gene1,gene2,gene3...>")
      .text(
        """
          |  List of genes for which to infer the putative regulators.
        """.stripMargin)
      .action{ case (genes, cfg) => cfg.modify(_.inf.each.targets).setTo(genes.toSet) }

  private val xgbParam =
    opt[(String, String)]("xgb-param").abbr("p")
      .optional
      .unbounded
      .text(
        s"""
           |  Add or overwrite an XGBoost booster parameter. Default parameters are:
           |${DEFAULT_BOOSTER_PARAMS.toSeq.sortBy(_._1).map{ case (k, v) => s"  * $k\t->\t$v" }.mkString("\n")}
          """.stripMargin)
      .action{ case ((key, value), cfg) => cfg.modify(_.inf.each.boosterParams).using(_.updated(key, value)) }

  private val nrBoostingRounds =
    opt[Int]("nr-boosting-rounds").abbr("r")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  Set the number of boosting rounds. Default: heuristically determined nr of boosting rounds.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.nrBoostingRounds).setTo(Some(nr)) }

  private val estimationGenes =
    opt[Seq[Gene]]("estimation-genes")
      .optional
      .valueName("<gene1,gene2,gene3...>")
      .validate(genes => if (genes.nonEmpty) success else failure("estimation-genes cannot be empty"))
      .text(
        """
          |  List of genes to use for estimating the nr of boosting rounds.
        """.stripMargin)
      .action{ case (genes, cfg)  => cfg.modify(_.inf.each.estimationSet).setTo(Right(genes.toSet))}

  private val nrEstimationGenes =
    opt[Int]("nr-estimation-genes")
      .optional
      .valueName("<nr>")
      .validate(nr => if (nr > 0) success else failure(s"nr-estimation-genes ($nr) should be larger than 0"))
      .text(
        s"""
          |  Nr of randomly selected genes to use for estimating the nr of boosting rounds. Default: $DEFAULT_ESTIMATION_SET.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.estimationSet).setTo(Left(nr)) }

  private val regularize =
    opt[Boolean]("regularize")
      .optional
      .valueName("<true/false>")
      .text(
        """
          |  Flag whether to enable or disable regularization (using the triangle method). Default: true.
          |  When true, only regulations approved by the triangle method will be emitted.
          |  When false, the regularization labels will be included as a 4th column.
        """.stripMargin)
      .action{ case (reg, cfg) => cfg.modify(_.inf.each.regularize).setTo(reg) }

  private val truncate =
    opt[Int]("truncate")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  Only keep the specified number regulations with highest importance score. Default: unlimited.
          |  (Motivated by the 100.000 regulations limit for the DREAM challenges.)
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.truncate).setTo(Some(nr)) }

  private val nrPartitions =
    opt[Int]("nr-partitions").abbr("par")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  The number of Spark partitions used to infer the GRN. Default: nr of available processors.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.nrPartitions).setTo(Some(nr)) }

  private val dryRun =
    opt[Unit]("dry-run")
      .optional
      .text(
        """
          |  Inference nor auto-config will launch if this flag is set. Use for parameters inspection.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.goal).setTo(DRY_RUN) }

  private val configRun =
    opt[Unit]("cfg-run")
      .optional
      .text(
        """
          |  Auto-config will launch, inference will not if this flag is set. Use for config testing.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.goal).setTo(CFG_RUN) }

  private val transposed =
    opt[Unit]("transposed")
      .optional
      .text(
        """
          |  Set this flag if the input rows=observations and cols=genes.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.inputTransposed).setTo(true) }

  private val report =
    opt[Boolean]("report")
      .optional
      .valueName("<true/false>")
      .text(
        """
          |  Set whether to write a report about the inference run to file.
        """.stripMargin)
      .action{ case (report, cfg) => cfg.modify(_.inf.each.report).setTo(report) }

  private val iterated =
    opt[Unit]("iterated")
      .optional
      .hidden
      .text(
        """
          |  Indicates using the iterated DMatrix API instead of using cached DMatrix copies of the CSC matrix.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.iterated).setTo(true) }

  head("GRNBoost", "0.1")

  help("help").abbr("h")
    .text(
      """
        |  Prints this usage text.
      """.stripMargin)

  version("version").abbr("v")
    .text(
      """
        |  Prints the version number.
      """.stripMargin)

  cmd("infer")
    .action{ (_, cfg) => cfg.copy(inf = Some(InferenceConfig())) }
    .text(
      """
        |Launch GRN inference.
      """.stripMargin)
    .children(
      input, skipHeaders, output, regulators, delimiter, outputFormat, sample, targets, xgbParam,
      regularize, truncate, nrBoostingRounds, nrPartitions, transposed,
      estimationGenes, nrEstimationGenes,
      iterated, dryRun, configRun)

  override def renderingMode = OneColumn

  override def terminate(exitState: Either[String, Unit]): Unit = ()

  def apply(args: String*): Option[Config] = parse(args, Config())

  def parse(args: Array[String]) = apply(args: _*)

}

/*

TODO
- importance score: SUM_GAIN, AVG_GAIN,
- outCols: gain, freq

*/

trait BaseConfig extends Product {

  override def toString = this.toMap.mkString("\n")

}

case class Config(inf: Option[InferenceConfig] = None,
                  bla: Option[TestConfig]      = None) extends BaseConfig

sealed trait Goal
case object DRY_RUN extends Goal // only inspect configuration params
case object CFG_RUN extends Goal // add estimation of boosting rounds to params
case object INF_RUN extends Goal // perform the inference

sealed trait Format
case object LIST    extends Format
case object MATRIX  extends Format
case object PARQUET extends Format

object Format {

  def apply(s: String): Format = s.toLowerCase match {
    case "list"    => LIST
    case "matrix"  => MATRIX
    case "parquet" => PARQUET
    case _         => ???
  }

}

/**
  * @param input Required. The input file.
  * @param skipHeaders The number of header lines to ignore in the input file.
  * @param inputTransposed Flag to indicate the input file is transposed, i.e. FIXME description.
  * @param delimiter The delimiter used to parse the input file. Default: TAB.
  * @param regulators File containing regulator genes. Expects on gene per line.
  * @param output Required. The output file.
  * @param outputFormat The output format: list, matrix or parquet.
  * @param sampleSize The nr of randomly sampled observations to take into account in the inference.
  * @param nrPartitions The nr of Spark partitions to use for inference.
  * @param truncate The max nr of regulatory connections to return.
  * @param nrBoostingRounds The nr of boosting rounds.
  * @param estimationSet A nr or set of genes to estimate the nr of boosting rounds, if no nr is specified.
  * @param nrFolds The nr of folds to use to estimate the nr of boosting rounds with cross validation.
  * @param regularize Use triangle cutoff to prune the inferred regulatory connections.
  * @param targets A Set of target genes to infer the regulators for. Defaults to all.
  * @param boosterParams Booster parameters.
  * @param goal The goal: dry-run, configuration or inference.
  * @param report Write a report to file.
  * @param iterated Hidden, experimental. Use iterated DMatrix initialization instead of copying.
  */
case class InferenceConfig(input:             Option[File]            = None,
                           skipHeaders:       Int                     = 0,
                           inputTransposed:   Boolean                 = false,
                           delimiter:         String                  = "\t",
                           regulators:        Option[File]            = None,
                           output:            Option[File]            = None,
                           outputFormat:      Format                  = LIST,
                           sampleSize:        Option[Int]             = None,
                           nrPartitions:      Option[Int]             = None,
                           truncate:          Option[Int]             = None,
                           nrBoostingRounds:  Option[Int]             = None,
                           estimationSet:     Either[Int, Set[Gene]]  = Left(DEFAULT_ESTIMATION_SET),
                           nrFolds:           Int                     = DEFAULT_NR_FOLDS,
                           regularize:        Boolean                 = true,
                           targets:           Set[Gene]               = Set.empty,
                           boosterParams:     BoosterParams           = DEFAULT_BOOSTER_PARAMS,
                           goal:              Goal                    = INF_RUN,
                           report:            Boolean                 = true,
                           iterated:          Boolean                 = false) extends BaseConfig

case class TestConfig(bla: String = "bla")