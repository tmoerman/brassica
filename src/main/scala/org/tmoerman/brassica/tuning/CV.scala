package org.tmoerman.brassica.tuning

import java.lang.Math.min

import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object CV {

  type FoldNr = Int

  /**
    * @param nrFolds
    * @param nrSamples
    * @param seed
    * @return Returns a Map of (train, test) sets by fold id.
    */
  def makeCVSets(nrFolds: Count,
                 nrSamples: Count,
                 seed: Long = DEFAULT_SEED): List[(Array[CellIndex], Array[CellIndex])] = {

    val foldSlices = makeFoldSlices(nrFolds, nrSamples, seed)

    foldSlices
      .keys
      .toList
      .map(fold => {
        val (train, test) = foldSlices.partition(_._1 != fold)

        (train.values.flatten.toArray, test.values.flatten.toArray)})
  }

  /**
    * @param nrFolds The nr of folds.
    * @param nrSamples The nr of samples to slice into folds.
    * @param seed A seed for the random number generator.
    * @return Returns a Map of cell indices by fold id.
    */
  def makeFoldSlices(nrFolds: Count,
                     nrSamples: Count,
                     seed: Long = DEFAULT_SEED): Map[FoldNr, List[CellIndex]] = {

    assert(nrFolds > 1, s"nr folds must be greater than 1 (specified: $nrFolds)")

    assert(nrSamples > 0, s"nr samples must be greater than 0 (specified: $nrSamples)")

    val denominator = min(nrFolds, nrSamples)

    rng(seed)
      .shuffle((0 until nrSamples).toList)
      .zipWithIndex
      .map{ case (cellIndex, idx) => (cellIndex, idx % denominator) }
      .groupBy{ case (_, fold) => fold }
      .mapValues(_.map(_._1).sorted)
  }

}