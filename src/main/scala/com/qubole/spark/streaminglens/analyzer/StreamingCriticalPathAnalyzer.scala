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

import com.qubole.spark.streaminglens.common.{MicroBatchContext, QueryDescription, StreamingState}
import com.qubole.spark.streaminglens.common.results.StreamingCriticalPathResults
import com.qubole.spark.streaminglens.config.StreamingLensConfig
import com.qubole.spark.streaminglens.helper.JobOverlapHelper

class StreamingCriticalPathAnalyzer(queryDesc: QueryDescription,
                                    streamingLensConfig: StreamingLensConfig) {

  private val expectedMicroBatchSLA = queryDesc.expectedMicroBatchSLAMillis

  def calculateCriticalTime(microBatchContext: MicroBatchContext): Long = {

    // wall clock time, batchEnd - bacthStart
    val batchTotalTime = microBatchContext.endTime - microBatchContext.startTime

    // wall clock time per Job. Aggregated
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(microBatchContext)

    // Minimum time required to run a job even when we have infinite number
    // of executors, essentially the max time taken by any task in the stage.
    // which is in the critical path. Note that some stages can run in parallel
    // we cannot reduce the job time to less than this number.
    // Aggregating over all jobs, to get the lower bound on this time.
    val criticalPathTime = JobOverlapHelper.criticalPathForAllJobs(microBatchContext)

    val driverTimeJobBased = batchTotalTime - jobTime

    driverTimeJobBased + criticalPathTime

  }

  def analyze(microBatchContext: MicroBatchContext): StreamingCriticalPathResults = {

    val batchRunningTime = microBatchContext.endTime - microBatchContext.startTime
    val criticalTime = calculateCriticalTime(microBatchContext)

    getCriticalPathResults(batchRunningTime, criticalTime, microBatchContext)

  }

  private def getCriticalPathResults(batchRunningTime: Long,
                                      criticalTime: Long,
                                      microBatchContext: MicroBatchContext):
  StreamingCriticalPathResults = {

    val streamingLensResults = (batchRunningTime, expectedMicroBatchSLA, criticalTime) match {
      case (_, _, _)
        if batchRunningTime <= expectedMicroBatchSLA * streamingLensConfig.laggingThreshold =>
        StreamingCriticalPathResults(
          expectedMicroBatchSLA, batchRunningTime, criticalTime, StreamingState.OVERPROVISIONED)
      case(_, _, _)
        if expectedMicroBatchSLA * streamingLensConfig.laggingThreshold < batchRunningTime &&
          batchRunningTime <= expectedMicroBatchSLA * streamingLensConfig.okayThreshold =>
        StreamingCriticalPathResults(
          expectedMicroBatchSLA, batchRunningTime, criticalTime, StreamingState.OPTIMUM)
      case(_, _, _)
        if batchRunningTime > expectedMicroBatchSLA * streamingLensConfig.okayThreshold &&
          criticalTime <= expectedMicroBatchSLA * streamingLensConfig.criticalPathThreshold =>
        StreamingCriticalPathResults(
          expectedMicroBatchSLA, batchRunningTime, criticalTime, StreamingState.UNDERPROVISIONED)
      case(_, _, _)
        if batchRunningTime > expectedMicroBatchSLA * streamingLensConfig.okayThreshold &&
          criticalTime > expectedMicroBatchSLA * streamingLensConfig.criticalPathThreshold =>
        StreamingCriticalPathResults(
          expectedMicroBatchSLA, batchRunningTime, criticalTime, StreamingState.UNHEALTHY)
    }
    streamingLensResults
  }

}