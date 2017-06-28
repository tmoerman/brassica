package org.aertslab.grnboost.xgbfi

import org.aertslab.grnboost._

/**
  * @author Thomas Moerman
  */
object ParseModelDump {

  def apply(modelDump: ModelDump) = {


    ???
  }

}

trait Tree

object Tree {

  def apply(treeDump: TreeDump): Tree =
    treeDump.split(":") match {
      case Array(nr, spec) => if (spec.startsWith("[")) {
        ???
      } else {
        ???
      }
    }

}

case class Node(gene: Gene,
                number: Int,
                splitValue: Float,
                l: Tree,
                r: Tree,
                missing: Count,
                freq: Frequency,
                gain: Gain,
                cover: Cover)

case class Leaf(value: Double,
                cover: Cover)

case class FeatureInteraction(featureIndex: Int,
                              depth: Count,
                              gain: Gain,
                              cover: Cover,
                              freq: Frequency,
                              weightedFreq: Float,
                              avgWeightedFreq: Float,
                              avgGain: Float,
                              expectedGain: Float,
                              treeIndex: Int,
                              avgTreeIndex: Float,
                              treeDepth: Int
                              //average
                              )