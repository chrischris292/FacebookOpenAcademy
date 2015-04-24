package io.prediction.opennlp.engine

import io.prediction.controller.P2LAlgorithm
import opennlp.maxent.GIS
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import io.prediction.data.storage.DataMap


class Algorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  def train(sc: SparkContext, data: PreparedData): Model = {
    println(data.dataIndexer)
    Model(GIS.trainModel(ap.iteration, data.dataIndexer, ap.smoothing))
  }

  def predict(model: Model, query: Query): PredictedResult = {
    val interest = Interest(
      model.gis.getBestOutcome(model.gis.eval(query.sentence.split(" "))).toInt
    )
    PredictedResult(interest.toString)
  }

  override def batchPredict(model: Model, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    qs.sparkContext.parallelize(
      qs.collect().map { case (index, query) =>
        (index, predict(model, query))
      })
  }

}