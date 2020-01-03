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

package com.qubole.spark.streaminglens.listener

import com.qubole.spark.streaminglens.{QueryInsightsManager, StreamingLens}
import com.qubole.spark.streaminglens.common.QueryDescription
import com.qubole.spark.streaminglens.config.StreamingLensConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class QueryProgressListener(streamingLens: StreamingLens,
                            streamingLensConfig: StreamingLensConfig)
  extends StreamingQueryListener with Logging {

  private lazy val streamingAppTracker = streamingLens.getStreamingAppTracker()

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    try {
      val expectedMicroBatchSLA = streamingAppTracker.getExpectedMicroBatchSLA(
        Option(event.name).getOrElse(event.id.toString))
        .getOrElse(streamingLensConfig.expectedMicroBatchSLAMillis)
      if (streamingAppTracker.queryInsightsManagerMap.contains(event.id)) {
        logWarning(s"Found an active query insights manager for query ${event.id}.")
      }
      streamingAppTracker.queryInsightsManagerMap.putIfAbsent(
        event.id, new QueryInsightsManager(
          QueryDescription(queryName = event.name, queryId = event.id, queryRunId = event.runId,
            expectedMicroBatchSLAMillis = expectedMicroBatchSLA),
          streamingLensConfig, streamingLens))
    } catch {
      case e: Exception =>
        logWarning("Error occurred on QueryStartedEvent. Insights may not be reported." +
          e.getMessage)
    }
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    try {
      val queryInsightsManagerOption = streamingAppTracker.queryInsightsManagerMap.get(
        event.progress.id)
      queryInsightsManagerOption match {
        case Some(queryInsightsManager) =>
          queryInsightsManager.updateLastProgress(event.progress)
          queryInsightsManager.triggerAnalysis()
        case None =>
          logWarning(s"QueryInsightsManager for query ${event.progress.id} not found. " +
            s"Insights for query ${event.progress.id} may not be published")
      }
    } catch {
      case e: Exception =>
        logWarning("Error occurred on QueryProgressEvent. Insights may not be reliable." +
          e.getMessage)
    }
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    try {
      val queryInsightsManagerOption = streamingAppTracker.queryInsightsManagerMap.get(event.id)
      queryInsightsManagerOption match {
        case Some(queryInsightsManager) =>
          queryInsightsManager.triggerShutDown()
          streamingAppTracker.queryInsightsManagerMap.remove(event.id)
        case None =>
          logWarning(s"QueryInsightsManager for query ${event.id} not found. " +
            s"Insights for query ${event.id} may not be published")
      }
    } catch {
      case e: Exception =>
        logWarning("Error occurred on QueryTerminatedEvent. Clean up may not have happened" +
          e.getMessage)
    }
  }


}