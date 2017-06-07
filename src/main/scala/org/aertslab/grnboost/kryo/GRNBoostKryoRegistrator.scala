package org.aertslab.grnboost.kryo

import com.esotericsoftware.kryo.Kryo
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.serializer.KryoRegistrator

/**
  * @author Thomas Moerman
  */
class GRNBoostKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Booster])
  }

}