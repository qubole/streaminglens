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

package com.qubole.spark.streaminglens

import java.util.concurrent.{Executors, ExecutorService, TimeUnit}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.qubole.spark.streaminglens.analyzer.StreamingQueryAnalyzer
import com.qubole.spark.streaminglens.common.{BatchDescription, QueryDescription, QueryProgress, StreamingState}
import com.qubole.spark.streaminglens.common.results.StreamingLensResults
import com.qubole.spark.streaminglens.config.StreamingLensConfig
import com.qubole.spark.streaminglens.helper.{StreamingLensReportingHelper, StreamingLensRetriesHelper}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.sql.streaming.qubole.streaminglens.metrics.StreamingLensMetricsReporter


/**
 * Insights Manager for a single streaming query. It manages analysis, metrics reporting
 * and sending events.
 *
 * @param queryDescription Description of key query properties like name, id, expectedSLA, sources
 * and sink
 * @param streamingLensConfig Various streamingLens configurations
 * @param streamingLens Parent class which manages
 */

class QueryInsightsManager(queryDescription: QueryDescription,
                           streamingLensConfig: StreamingLensConfig,
                           streamingLens: StreamingLens)
  extends StreamingLensRetriesHelper with Logging {

  private val queryIdentifier: String = Option(queryDescription.queryName)
    .getOrElse(queryDescription.queryId.toString)

  private lazy val streamingAppTracker = streamingLens.getStreamingAppTracker()

  private val insightsThreadPool: Option[ExecutorService] =
    createThreadPool(s"streaminglens-$queryIdentifier")

  val streamingLensResultsBuffer = new mutable.Queue[StreamingLensResults]()

  private val streamingLensMetricsReporter: Option[StreamingLensMetricsReporter] =
    initiateMetricsReporter()

  private val eventsReporter: Option[StreamingLensReportingHelper] = initiateEventsReporter()

  private lazy val streamingQueryAnalyzer = createStreamingQueryAnalyzer()

  @volatile
  var lastProgress: StreamingQueryProgress = _

  var lastAnalyzedTimeMills: Long = _

  var lastPurgedBatch: Long = -1

  private def createStreamingQueryAnalyzer() : StreamingQueryAnalyzer = {
    updateQueryDescription()
    new StreamingQueryAnalyzer(streamingAppTracker,
      streamingLensConfig, queryDescription)
  }

  private def updateQueryDescription(): Unit = {
    queryDescription.sourcesDesc = lastProgress.sources.map(_.description).toList
    queryDescription.sinkDesc = lastProgress.sink.description
  }


  private def createThreadPool(threadName: String): Option[ExecutorService] = {
    try {
      val threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadName)
        .build()
      logDebug("Successfully created streaming lens thread pool")
      Some(Executors.newSingleThreadExecutor(threadFactory))
    } catch {
      case e: Exception =>
        throw new SparkException("Error occurred while initializing StreamingLens ThreadPool  " +
          "Won't report streaminglens insights " + e.getMessage)
    }
  }

  private def initiateMetricsReporter() : Option[StreamingLensMetricsReporter] = {
    try {
      val metricsReporter = new StreamingLensMetricsReporter(queryIdentifier,
        streamingLensResultsBuffer)
      logDebug("Successfully registered streaming lens metrics reporter")
      Some(metricsReporter)
    } catch {
      case e: Exception =>
        logWarning("Error in registering StreamingLens Metrics Reporter. " +
          "Won't display streaming lens results as metrics" + e.getMessage)
        None
    }
  }

  private def initiateEventsReporter(): Option[StreamingLensReportingHelper] = {
    if (streamingLensConfig.enableReporting) {
      try {
        val eventsReporter = new StreamingLensReportingHelper(
          streamingLensConfig, streamingLensResultsBuffer, queryDescription)
        Some(eventsReporter)
      } catch {
        case e: Exception =>
          logWarning("Error in registering StreamingLens Events Reporter. " + e.getMessage)
          None
      }
    } else {
      None
    }
  }

  def updateLastProgress(queryProgress: StreamingQueryProgress): Unit = {
    lastProgress = queryProgress
  }

  def triggerAnalysis(): Unit = {
    if (insightsThreadPool.isDefined && !insightsThreadPool.get.isTerminated) {
      insightsThreadPool.get.submit(analysisTask)
    } else {
      logWarning("Insights Thread Inactive. Won't report insights")
    }
  }


  val analysisTask = new Runnable {
    override def run(): Unit = {
      val queryProgress = getLastProgress()
      try {
        startStreamingAnalysis(queryProgress)
        resetRetries()
      } catch {
        case e: Exception =>
          logWarning(s"Streaming Lens failed " + e.getMessage)
          evaluateRetries()
      } finally {
        doCleanupIfNecessary(queryProgress)
      }
    }
  }

  override def evaluateRetries(): Unit = {
    if (incrementRetries() >= streamingLensConfig.maxRetries) {
      logInfo("Max retries reached. Attempting to stop StreamingLens")
      val shutdownFuture = Future {
        streamingLens.stopStreamingLens()
      }
      shutdownFuture onComplete {
        case Success(_) =>
          logInfo("Successfully shutdown StreamingLens")
        case Failure(e) =>
          logWarning("Error occured while shutting down StreamingLens" + e.getMessage)
      }
    }
  }

  private def startStreamingAnalysis(queryProgress: QueryProgress): Unit = {
    val currentTime = System.currentTimeMillis()
    if (shouldTriggerAnalysis(currentTime)) {
      val insights = streamingQueryAnalyzer.analyze(queryProgress)
      logResultsIfNecessary(insights)
      lastAnalyzedTimeMills = currentTime
      if (insights.streamingCriticalPathResults.streamingQueryState.equals(StreamingState.ERROR)) {
        throw new SparkException("Unexpected Error or Timeout occurred during Analysis")
      }
      streamingLensResultsBuffer.enqueue(insights)
      eventsReporter.foreach(_.sendEvent())
    }
  }

  private def shouldTriggerAnalysis(currentTime: Long): Boolean = {
    currentTime - lastAnalyzedTimeMills >= streamingLensConfig.analysisIntervalMinutes * 60 * 1000
  }

  private def getLastProgress(): QueryProgress = synchronized {
    QueryProgress(lastProgress.batchId,
      lastProgress.id,
      lastProgress.timestamp,
      lastProgress.numInputRows,
      lastProgress.processedRowsPerSecond)
  }

  private def logResultsIfNecessary(results: StreamingLensResults): Unit = {

    if (streamingLensConfig.shouldLogResults) {

      val logOutput = new mutable.StringBuilder()
      logOutput.append(
        s"""
           | |||||||||||||||||| StreamingLens Inisights |||||||||||||||||||||||||
           | BatchId: ${results.batchId}
           | Analysis Time: ${pd(results.analysisTime)}
           | Expected Micro Batch SLA: ${pd(results.streamingCriticalPathResults.expectedMicroBatchSLA)}
           | Batch Running Time: ${pd(results.streamingCriticalPathResults.batchRunningTime)}
           | Critical Time: ${pd(results.streamingCriticalPathResults.criticalTime)}
           | Streaming Query State: ${results.streamingCriticalPathResults.streamingQueryState.toString}
           | ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
          """.stripMargin)
      logInfo(logOutput.toString())

    }

  }

  private def pd(millis: Long) : String = {

    "%02ds %03dms".format(TimeUnit.MILLISECONDS.toSeconds(millis),
      millis - TimeUnit.MILLISECONDS.toSeconds(millis)*1000)
  }

  private def doCleanupIfNecessary(queryProgress: QueryProgress): Unit = {
    if (queryProgress.numInputRows != 0 &&
      queryProgress.batchId - lastPurgedBatch >= streamingLensConfig.maxBatchesRetention) {
      val batchDescription = BatchDescription(queryProgress.queryId, queryProgress.batchId)
      streamingAppTracker.doCleanUp(batchDescription)
      lastPurgedBatch = queryProgress.batchId
    }
    while (streamingLensResultsBuffer.size > streamingLensConfig.maxResultsRetention) {
      streamingLensResultsBuffer.dequeue()
    }
  }

  def triggerShutDown(): Unit = {
    try {
      insightsThreadPool match {
        case Some(threadPool) =>
          if (!threadPool.isTerminated) {
            threadPool.shutdownNow()
          }
        case None =>
        // Do Nothing
      }
      streamingLensMetricsReporter match {
        case Some(metricsReporter) =>
          metricsReporter.removeSource()
        case None =>
        // Do Nothing
      }
      if (streamingAppTracker.expectedMicroBatchSLAMap.get(queryIdentifier).isDefined) {
        streamingAppTracker.removeExpectedMicroBatchSLA(queryIdentifier)
      }
    } catch {
      case e: Exception =>
        logWarning(s"Error in shutting down StreamingLens Insights Manager for query " +
          s"${queryIdentifier} " + e.getMessage)
    }

  }

}
