package org.tmoerman.brassica

/**
  * @author Thomas Moerman
  */
object DataPaths {

  val res = "src/test/resources/"

  val zeisel = "/Users/tmo/Work/ghb2016/data/zeisel/"

  val genie3 = res + "genie3/data.txt"

  val dream5wd = "/Users/tmo/Work/ghb2016/data/Dream_challenge/GNI/original data"

  val dream5ecoli   = dream5(dream5wd + "/Ecoli",       "/ecoli")
  val dream5saureus = dream5(dream5wd + "/Saureus",     "/saureus")
  val dream5yeast   = dream5(dream5wd + "/Scerevisiae", "/yeast")

  private[this] def dream5(dir: String, species: String) =
    Seq(
      s"${species}_data.tsv",
      s"${species}_gene_names.tsv",
      s"${species}_tf_names.tsv")
    .map(dir + _)

}