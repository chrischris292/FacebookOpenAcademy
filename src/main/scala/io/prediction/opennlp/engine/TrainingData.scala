package io.prediction.opennlp.engine

import opennlp.model.DataIndexer
import scala.collection.Map

//case class TrainingData(dataIndexer: DataIndexer, map: Map[Integer, String]) extends Serializable
case class TrainingData(dataIndexer: DataIndexer) extends Serializable
