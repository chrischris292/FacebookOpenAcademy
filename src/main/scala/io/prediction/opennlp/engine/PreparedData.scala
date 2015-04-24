package io.prediction.opennlp.engine

import opennlp.model.DataIndexer
import scala.collection.Map

//case class PreparedData(dataIndexer: DataIndexer, map: Map[Integer, String]) extends Serializable
case class PreparedData(dataIndexer: DataIndexer) extends Serializable
