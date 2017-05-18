package org.tmoerman.grnboost.util

import scala.util.Random
import org.tmoerman.grnboost._

/**
  * @author Thomas Moerman
  */
object RankUtils {

  type Position = Int
  type Rank     = Int

  /**
    * @param values The values to turn into ranks.
    * @param base The base (0, 1) rank, default == 1.
    * @param rng A random number generator for shuffling.
    * @tparam V Generic type for ordered values
    * @return Returns a Seq of ranks.
    */
  def toRankings[V](values: Seq[V],
                    base: Rank = 1,
                    rng: Random = random(777))(implicit o: Ordering[V]): Seq[Rank] =
    values
      .zipWithIndex
      .sortBy{ case (v, p) => v }
      .zipWithIndex
      .groupBy{ case ((v, p), r) => v }
      .foldLeft(List[(Position, Rank)]()){ case (acc, (v, group)) => {
        val ranks = group.map{ case ((v, p), r) => r + base }
        val shuffledRanks = rng.shuffle(ranks)

        val positions = group.map{ case ((v, p), r) => p }
        val shuffledPositions = rng.shuffle(positions)

        acc ++ (shuffledPositions zip shuffledRanks)
      }}
      .sortBy{ case (p, _) => p}
      .map(_._2)

}