package io.prediction.opennlp.engine

import io.prediction.controller.{EmptyEvaluationInfo, EmptyParams, PDataSource}
import io.prediction.data.storage.Storage
import io.prediction.opennlp.engine.Interest.Interest
import opennlp.maxent.BasicEventStream
import opennlp.model.OnePassDataIndexer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

class DataSource(val dsp: DataSourceParams) extends PDataSource[
  TrainingData,
  EmptyEvaluationInfo,
  Query,
  Interest] {

  val Separator = " "

  override def readTraining(sc: SparkContext): TrainingData = {
    val trainingTreeStrings = allPhraseandInterests(sc)
    phraseAndInterestToTrainingData(trainingTreeStrings)
  }

  override def readEval(
    sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, Interest)])] = {
    val shuffled = Random.shuffle(allPhraseandInterests(sc))
    val (trainingSet, testingSet) =
      shuffled.splitAt((shuffled.size*0.9).toInt)

    val trainingData = phraseAndInterestToTrainingData(trainingSet)

    val qna = testingSet.map { line =>
      val lastSpace = line.lastIndexOf(Separator)
      println("MAP")
      println(Interest(line.substring(lastSpace + 1).toInt))

      (Query(line.substring(0, lastSpace)), Interest(line.substring(lastSpace + 1).toInt))
    }
    Seq((trainingData, EmptyParams(), sc.parallelize(qna)))
  }

  private def allPhraseandInterests(sc: SparkContext): Seq[String] = {
    val events = Storage.getPEvents().find(appId = dsp.appId, entityType = Some("phrase"))(sc)

    events.map { event =>
      val phrase = event.properties.get[String]("phrase")
      val Interest = event.properties.get[String]("Interest")

      s"$phrase $Interest"
    }.collect().toSeq
  }

  private def phraseAndInterestToTrainingData(phraseAndInterests: Seq[String]) = {
    val eventStream = new BasicEventStream(new SeqDataStream(phraseAndInterests), Separator)
    val dataIndexer = new OnePassDataIndexer(eventStream, dsp.cutoff)

    TrainingData(dataIndexer)
  }
}
