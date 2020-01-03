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

package com.qubole.spark.streaminglens.analyzer

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.TimeoutException

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.qubole.spark.streaminglens.StreamingAppTracker
import com.qubole.spark.streaminglens.common.{BatchDescription, MicroBatchContext, QueryDescription, QueryProgress, StreamingState}
import com.qubole.spark.streaminglens.common.results.{StreamingCriticalPathResults, StreamingLensResults}
import com.qubole.spark.streaminglens.config.StreamingLensConfig

import org.apache.spark.internal.Logging


class StreamingQueryAnalyzer(streamingAppTracker: StreamingAppTracker,
                             streamingLensConfig: StreamingLensConfig,
                             queryDesc: QueryDescription)
  extends Logging {

  private var lastAnalyzedBatchId: Long = -1

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  private val streamingCriticalPathAnalyzer = new StreamingCriticalPathAnalyzer(
    queryDesc, streamingLensConfig)

  def analyze(queryProgress: QueryProgress): StreamingLensResults = {
    val batchId = queryProgress.batchId

    val insights = {

      if (shouldDoStreamingLensAnalysis(queryProgress, lastAnalyzedBatchId)) {

        val batchStartAndEndTimes = computeBatchStartAndEndTime(queryProgress)

        if (batchStartAndEndTimes._1 != 0 && batchStartAndEndTimes._2 != 0) {

          val batchRunningTime = batchStartAndEndTimes._2 - batchStartAndEndTimes._1
          val batchDescription = BatchDescription(queryProgress.queryId, batchId)

          val microBatchContext = MicroBatchContext.createMicroBatchContext(
            streamingAppTracker,
            batchDescription,
            batchStartAndEndTimes._1,
            batchStartAndEndTimes._2)

          val future = Future {

            logDebug(s"Starting Analysis for batch ${batchId}")
            val analysisStartTime = System.currentTimeMillis()

            val streamingCriticalPathResults = streamingCriticalPathAnalyzer
              .analyze(microBatchContext)
            logDebug(s"Completed Analysis for batch ${batchId}")

            val analysisEndTime = System.currentTimeMillis()
            lastAnalyzedBatchId = queryProgress.batchId
            StreamingLensResults(
              batchId, analysisEndTime - analysisStartTime,
              streamingCriticalPathResults)

          }
          // scalastyle:off
          try {
            Await.result(future, streamingLensConfig.maxAnalysisTimeSeconds second)
          } catch {
            case t: TimeoutException =>
              logWarning("Timed Out while Performing Analysis")
              StreamingLensResults(batchId, 0, StreamingCriticalPathResults(
                queryDesc.expectedMicroBatchSLAMillis, batchRunningTime, 0, StreamingState.ERROR))
            case e: Exception =>
              logWarning("Error Occured while Performing Analysis" + e.getMessage)
              StreamingLensResults(batchId, 0,
                StreamingCriticalPathResults(
                  queryDesc.expectedMicroBatchSLAMillis, batchRunningTime, 0, StreamingState.ERROR))
          }

          // scalastyle:on

        } else {
          logDebug(s"No Analysis for batch ${batchId}. Unable to compute batch start and" +
            s"end times ..." +
            s"NumInputRows = ${queryProgress.numInputRows}, " +
            s"ProcessedRPS = ${queryProgress.processedRowsPerSecond}")
          StreamingLensResults(batchId, 0, StreamingCriticalPathResults(
            queryDesc.expectedMicroBatchSLAMillis, 0, 0, StreamingState.NONEWBATCHES))
        }
      } else {
        StreamingLensResults(batchId, 0, StreamingCriticalPathResults(
          queryDesc.expectedMicroBatchSLAMillis, 0, 0, StreamingState.NONEWBATCHES))
      }
    }
    insights
  }

  private def computeBatchStartAndEndTime(queryProgress: QueryProgress): (Long, Long) = {
    if (queryProgress.numInputRows != 0 && queryProgress.processedRowsPerSecond != 0) {
      logDebug(s"Starting time calculations for batch ${queryProgress.batchId}")
      val batchStartTime = convertTimeStampToMillis(queryProgress.timestamp)
      val batchRunningTime = (queryProgress.numInputRows /
        queryProgress.processedRowsPerSecond) * 1000
      val batchEndTime = (batchStartTime + batchRunningTime).toLong
      (batchStartTime, batchEndTime)
    } else {
      (0, 0)
    }
  }


  private def shouldDoStreamingLensAnalysis(lastProgress: QueryProgress,
    lastBatchId: Long): Boolean = {

    lastProgress.batchId - lastBatchId >= streamingLensConfig.minBatches
  }

  private def convertTimeStampToMillis(timestamp: String): Long = {
    val timeInMillis = timestampFormat.parse(timestamp).getTime()
    timeInMillis
  }



}
