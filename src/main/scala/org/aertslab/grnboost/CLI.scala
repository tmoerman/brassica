package org.aertslab.grnboost

import java.io.File

import scopt.OptionParser
import com.softwaremill.quicklens._
import scopt.RenderingMode.OneColumn

/**
  * Command line argument parser.
  *
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

  private val inputHeaders =
    opt[Int]("ignore-headers")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  The number of input file header lines to ignore. Default: 0.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.inputHeaders).setTo(nr) }

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
      .hidden
      .valueName("<list|matrix|parquet>")
      .validate(format =>
        if (Set("list", "matrix", "parquet").contains(format))
          success
        else
          failure(s"unknown output format (${format.toLowerCase}"))
      .text(
        """
          |  Output format. Default: list.
        """.stripMargin)
      .action{ case (format, cfg) => cfg.modify(_.inf.each.outputFormat).setTo(format) }

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
    opt[Seq[String]]("targets")
      .optional
      .valueName("gene1,gene2,gene3...")
      .text(
        """
          |  List of genes for which to infer the putative regulators.
        """.stripMargin)
      .action{ case (genes, cfg) => cfg.modify(_.inf.each.targets).setTo(genes) }

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
          |  Set the number of boosting rounds. Default: unlimited.
        """.stripMargin)
      .action{ case (nr, cfg) => cfg.modify(_.inf.each.nrBoostingRounds).setTo(Some(nr)) }

  private val earlyStop =
    opt[Boolean]("early-stop")
      .optional
      .valueName("<true/false>")
      .text(
        """
          |  Flag whether to enable or disable early stopping (limiting the nr of boosting rounds). Default: true.
        """.stripMargin)

  private val truncate =
    opt[Int]("truncate")
      .optional
      .valueName("<nr>")
      .text(
        """
          |  Only keep the specified number regulations with highest importance score. Default: unlimited.
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
          |  Inference will not launch if this flag is set. Use for input validation.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.dryRun).setTo(true) }

  private val transposed =
    opt[Unit]("transposed")
      .optional
      .hidden
      .text(
        """
          |  Set this flag if the input rows=observations and cols=genes.
        """.stripMargin)
      .action{ case (_, cfg) => cfg.modify(_.inf.each.inputTransposed).setTo(true) }

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
      input, inputHeaders, output, regulators, delimiter, outputFormat, sample, targets, xgbParam,
      truncate, nrBoostingRounds, earlyStop, nrPartitions, transposed, iterated, dryRun)

  cmd("test")
    .hidden
    .action{ (_, cfg) => cfg.copy(bla = Some(TestConfig())) }

  override def renderingMode = OneColumn

  override def terminate(exitState: Either[String, Unit]): Unit = ()

  def apply(args: String*): Option[Config] = parse(args, Config())

  def parse(args: Array[String]) = apply(args: _*)

}

/*

TODO

- nrRounds: AUTO or Int
- regularize: Boolean (--> use the triangle cutoffs)
- outCols: gain, freq

*/

trait BaseConfig extends Product {

  override def toString = this.toMap.mkString("\n")

}

case class Config(inf: Option[InferenceConfig] = None,
                  bla: Option[TestConfig]      = None) extends BaseConfig

case class InferenceConfig(input:             Option[File]  = None,
                           inputHeaders:      Int           = 0,
                           delimiter:         String        = "\t",
                           regulators:        Option[File]  = None,
                           output:            Option[File]  = None,
                           outputFormat:      String        = "list",
                           sampleSize:        Option[Int]   = None,
                           nrPartitions:      Option[Int]   = None,
                           truncate:          Option[Int]   = None,
                           nrBoostingRounds:  Option[Int]   = None,
                           boostingEarlyStop: Boolean       = true,
                           targets:           Seq[Gene]     = Seq.empty,
                           boosterParams:     BoosterParams = DEFAULT_BOOSTER_PARAMS,
                           inputTransposed:   Boolean       = false,
                           dryRun:            Boolean       = false,
                           iterated:          Boolean       = false) extends BaseConfig

case class TestConfig(bla: String = "bla")