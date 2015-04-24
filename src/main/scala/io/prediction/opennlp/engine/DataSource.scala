package io.prediction.opennlp.engine

import io.prediction.controller.{EmptyEvaluationInfo, EmptyParams, PDataSource}
import io.prediction.data.storage.Storage
import io.prediction.opennlp.engine.Interest.Interest
import opennlp.maxent.BasicEventStream
import opennlp.model.OnePassDataIndexer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import io.prediction.data.storage.DataMap
import Array._

import scala.util.Random

class DataSource(val dsp: DataSourceParams) extends PDataSource[
  TrainingData,
  EmptyEvaluationInfo,
  Query,
  Interest] {

  val Separator = " "

  override def readTraining(sc: SparkContext): TrainingData = {
    val trainingTreeStrings = allPhraseandInterests(sc)
    val categories = Storage.getPEvents().find(appId = dsp.appId, entityType = Some("categories"))(sc)
    val categoriesArr = categories.map { event =>
      val category = event.properties.get[String]("category")
      val categoryIndex = event.properties.get[Integer]("categoryIndex")
      println(category + " " + categoryIndex)
      (categoryIndex,category)
    }.collect()
    val categoriesMap = categoriesArr.groupBy(_._1).mapValues(_.map(_._2.toString()))
    println("start")
    println(categoriesMap)
    println(categoriesMap.get(62).toString().toString)
    println(categoriesMap.get(157).toString())
    println(categoriesMap.get(351).toString())
    println(categoriesMap.get(182).toString())
    println(categoriesMap.get(276).toString())
    println(categoriesMap.get(263).toString())

    phraseAndInterestToTrainingData(trainingTreeStrings)
  }
/*
  override def readEval(
    sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, Interest)])] = {
    val shuffled = Random.shuffle(allPhraseandInterests(sc))
    val (trainingSet, testingSet) =
      shuffled.splitAt((shuffled.size*0.9).toInt)

    val trainingData = phraseAndInterestToTrainingData(trainingSet)

    val qna = testingSet.map { line =>
      val lastSpace = line.lastIndexOf(Separator)
      (Query(line.substring(0, lastSpace)), Interest(line.substring(lastSpace + 1).toInt))
    }
    Seq((trainingData, EmptyParams() , sc.parallelize(qna)))
  }*/

  private def allPhraseandInterests(sc: SparkContext): Seq[String] = {

    //original
    val events = Storage.getPEvents().find(appId = dsp.appId, entityType = Some("phrase"))(sc)

    //added

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
