/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.qubole.spark.streaminglens.helper

import scala.collection.mutable
import scala.util.Try

import com.qubole.spark.streaminglens.common.{QueryDescription, StreamingState}
import com.qubole.spark.streaminglens.common.results.{AggregateStateResults, StreamingLensResults}
import com.qubole.spark.streaminglens.config.StreamingLensConfig
import com.qubole.spark.streaminglens.reporter.StreamingLensEventsReporterInterface
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging


class StreamingLensReportingHelper(streamingLensConfig: StreamingLensConfig,
                                  streamingLensResultsBuffer: mutable.Queue[StreamingLensResults],
                                  queryDescription: QueryDescription) extends Logging {

  val eventsReporter = createEventsReporter()

  var lastEventTimeMillis: Long = System.currentTimeMillis()

  var eventId: Long = _

  var lastReportedBatch : Long = -1

  private def createEventsReporter(): Option[StreamingLensEventsReporterInterface] = {
    try {
      // scalastyle:off
      val eventsReporterClass = Class.forName(
        streamingLensConfig.reporterClassName, true, getClass.getClassLoader)
      require(classOf[StreamingLensEventsReporterInterface].isAssignableFrom(eventsReporterClass))
      Some(eventsReporterClass
        .getDeclaredConstructor(classOf[Map[String, String]])
        .newInstance(streamingLensConfig.reportingOptions + ("queryId" -> queryDescription.queryId.toString))
        .asInstanceOf[StreamingLensEventsReporterInterface])
      // scalastyle:on
    } catch {
      case e: Exception =>
        throw new SparkException(s"Unable to initialize ${streamingLensConfig.reporterClassName}." +
          s" Won't be sending events" + e.getMessage)
    }
  }

  private def sendEventIfNecessary(): Unit = {
    val currentTime = System.currentTimeMillis()
    if (shouldSendEvent(currentTime)) {
      val aggregatedState = getAggregatedState()
      val eventInfo = getEventInfo(aggregatedState)
      val info = renderQueryInsightsEventInfo(eventInfo, currentTime)
      eventsReporter.foreach(_.sendInsightsEvent(info))
      eventId = eventId + 1
      lastEventTimeMillis = currentTime
      lastReportedBatch = streamingLensResultsBuffer.lastOption.map(_.batchId)
        .getOrElse(lastReportedBatch)
    }
  }

  private def renderQueryInsightsEventInfo(aggregateStateResults: AggregateStateResults,
                                           currentTimeMillis: Long): String = {

    def jsonValue(): JValue = {
      ("eventId" -> JInt(eventId)) ~
        ("name" -> JString(queryDescription.queryName)) ~
        ("runId" -> JString(queryDescription.queryRunId.toString)) ~
        ("eventTimeMillis" -> JInt(currentTimeMillis)) ~
        ("state" -> JString(aggregateStateResults.state)) ~
        ("displayText" -> JString(aggregateStateResults.recommendation))
    }
    prettyJson(jsonValue)
  }

  def sendEvent(): Unit = {
    try {
      sendEventIfNecessary()
    } catch {
      case e: Exception =>
        logWarning("Error Occured while reporting Event" + e.getMessage)
    }
  }

  private def getEventInfo(aggregatedState: Double): AggregateStateResults = {
    var state = ""
    val recommendation = new mutable.StringBuilder()
    aggregatedState match {
      case _ if aggregatedState == 0.0 =>
        recommendation.append("Streaming Query State: NO NEW BATCHES<br>")
        state = "NO NEW BATCHES"
      case _ if aggregatedState >= 1 && aggregatedState <= 1.5 =>
        recommendation.append(s"Streaming Query State: OVERPROVISIONED<br>")
        recommendation.append("Recommendations:<br>")
        val sourceSpecificRecommendations = getSourceSpecficRecommendations(
          queryDescription.sourcesDesc, StreamingState.OVERPROVISIONED)
        recommendation.append("> " + sourceSpecificRecommendations)
        recommendation.append("> Decrease the value of trigger Interval to process latest data<br>")
        recommendation.append("> You can decrease the number of executors if more than one " +
          "to reduce cost<br>")
        state = "NEEDS ATTENTION"
      case _ if aggregatedState > 1.5 && aggregatedState <= 2.5 =>
        recommendation.append(s"Streaming Query State: OPTIMUM<br>")
        recommendation.append("Recommendations:<br>")
        recommendation.append("Streaming Pipeline doing Okay. No Recommendations<br>")
        state = "GOOD"
      case _ if aggregatedState > 2.5 && aggregatedState <= 3.5 =>
        recommendation.append(s"Streaming Query State: UNDERPROVISIONED<br>")
        recommendation.append("Recommendations:<br>")
        recommendation.append("Scale up to ensure your pipeline doesn't fall behind<br>")
        state = "NEEDS ATTENTION"
      case _ if aggregatedState > 3.5 =>
        recommendation.append(s"Streaming Query State: UNHEALTHY<br>")
        recommendation.append("Recommendations:<br>")
        val sourceSpecificRecommendations = getSourceSpecficRecommendations(
          queryDescription.sourcesDesc, StreamingState.UNHEALTHY)
        recommendation.append("> " + sourceSpecificRecommendations)
        recommendation.append("> Use more efficient nodes<br>")
        recommendation.append("> Increase shuffle partitions if query has aggregations<br>")
        state = "AT RISK"
    }
    AggregateStateResults(state, recommendation.toString())
  }

  private def getSourceSpecficRecommendations(sources: List[String],
                                              state: StreamingState.Value): String = {

    val recommendation = new mutable.StringBuilder()

    state match {
      case StreamingState.OVERPROVISIONED =>
        sources.foreach { source =>
          val reco = source match {
            case s if s.contains("Kafka") =>
              "Increase maxOffsetsPerTrigger to ingest more data through Kafka Source<br>"
            case s if s.contains("File") =>
              "Increase maxFilesPerTrigger to ingest more data through File Source<br>"
            case _ =>
              "Tune source configurations to ingest more data<br>"
          }
          recommendation.append(reco)
        }
      case StreamingState.UNHEALTHY =>
        sources.foreach { source =>
          val reco = source match {
            case s if s.contains("Kafka") =>
              "Increase kafka partitions to ingest more data in parallel<br>"
            case s if s.contains("Kinesis") =>
              "Increase number of kinesis shards to ingest more data in parallel<br>"
            case _ =>
              "Tune source configurations to increase parallelism<br>"
          }
          recommendation.append(reco)
        }
    }
    recommendation.toString()
  }

  /** Computes aggregated state based on the states computed over the last reportimg period.
    * We exponentially weigh down old states based on the discount factor.
    */
  private def getAggregatedState(): Double = {
    val stateList = streamingLensResultsBuffer.toList.filter(_.batchId > lastReportedBatch).map(
      _.streamingCriticalPathResults.streamingQueryState.id).filter(_ != 0)
    if (stateList.isEmpty) {
      0.0
    } else {
      val exponents = (1 to stateList.size toArray).reverse
      val discountFactor = streamingLensConfig.discountFactor
      val combined = stateList.zip(exponents)
      val stateFraction = combined.foldLeft((0.0, 0.0)) { (sum, tuple) =>
        val multiplier = Math.pow(discountFactor, tuple._2)
        val numerator = sum._1 + (tuple._1 * multiplier)
        val denominator = sum._2 + multiplier
        (numerator, denominator)
      }
      Try(stateFraction._1 / stateFraction._2).getOrElse(0.0)
    }
  }

  private def shouldSendEvent(currentTimeMillis: Long): Boolean = {
    currentTimeMillis - lastEventTimeMillis >= streamingLensConfig.reportingIntervalMinutes * 60 * 1000
  }

  /** The compact JSON representation of this status. */
  protected def json(func: () => JValue): String = compact(render(func()))

  /** The pretty (i.e. indented) JSON representation of this status. */
  protected def prettyJson(func: () => JValue): String = pretty(render(func()))


}