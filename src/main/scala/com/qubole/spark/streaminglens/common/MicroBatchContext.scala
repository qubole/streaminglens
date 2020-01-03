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


package com.qubole.spark.streaminglens.common

import scala.collection.mutable

import com.qubole.spark.streaminglens.StreamingAppTracker
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, JobTimeSpan, StageTimeSpan, TimeSpan}


case class MicroBatchContext(batchDescription: BatchDescription,
                             executorMap: mutable.Map[String, ExecutorTimeSpan],
                             jobSQLExecIdMap: mutable.Map[Long, Long],
                             jobMap: mutable.Map[Long, JobTimeSpan],
                             startTime: Long,
                             endTime: Long)

object MicroBatchContext {
  def getMaxConcurrent[Span <: TimeSpan](map: mutable.Map[String, Span],
                                         microBatchContext: MicroBatchContext): Long = {

    // sort all start and end times on basis of timing
    val sorted = getSortedMap(map, microBatchContext)

    var count = 0L
    var maxConcurrent = 0L

    sorted.foreach(tuple => {
      count = count + tuple._2
      maxConcurrent = math.max(maxConcurrent, count)
    })

    // when running in local mode, we don't get
    // executor added event. Default to 1 instead of 0
    math.max(maxConcurrent, 1)
  }

  def getSortedMap[Span <: TimeSpan, T <: Any](map: mutable.Map[T, Span],
                                               microBatchContext: MicroBatchContext):
  Array[(Long, Long, Int)] = {

    // sort all start and end times on basis of timing
    map.values.flatMap(timeSpan => {
      val correctedEndTime = if (timeSpan.endTime == 0) {
        if (microBatchContext == null) {
          System.currentTimeMillis()
        } else microBatchContext.endTime
      } else timeSpan.endTime

      // Adding id of Stage.
      val id: Int = if (timeSpan.isInstanceOf[StageTimeSpan]) {
        timeSpan.asInstanceOf[StageTimeSpan].stageID
      } else -1
      Seq[(Long, Long, Int)]((timeSpan.startTime, 1L, id), (correctedEndTime, -1L, id))
    }).toArray
      .sortWith((t1: (Long, Long, Int), t2: (Long, Long, Int)) => {
        // for same time entry, we add them first, and then remove
        if (t1._1 == t2._1) {
          t1._2 > t2._2
        } else t1._1 < t2._1
      })
  }

  def getExecutorCores(microBatchContext: MicroBatchContext): Int = {
    if (microBatchContext.executorMap.values.lastOption.isDefined) {
      microBatchContext.executorMap.values.last.cores
    } else {
      // using default 1 core
      1
    }
  }

  def createMicroBatchContext(streamingAppTracker: StreamingAppTracker,
                              batchDescription: BatchDescription,
                              startTime: Long,
                              endTime: Long): MicroBatchContext = {
    val filteredJobMap = streamingAppTracker.jobMap.clone.retain(
      (key, value) => key >= getFirstJob(streamingAppTracker, batchDescription)
        && key <= getLastJob(streamingAppTracker, batchDescription))
    val jobIDs = filteredJobMap.keys.toList
    val filteredExecutorIDs = filterExecutorIDs(streamingAppTracker.jobIdToExecutorId.clone, jobIDs)
    val filteredExecutorMap = streamingAppTracker.executorMap.clone.retain(
      (key, value) => filteredExecutorIDs.contains(key))
    val filteredJobSQLExecIdMap = streamingAppTracker.jobSQLExecIDMap.clone.retain(
      (key, value) => jobIDs.contains(key)
    )
    MicroBatchContext(
      batchDescription,
      filteredExecutorMap,
      filteredJobSQLExecIdMap,
      filteredJobMap,
      startTime,
      endTime)

  }

  private def getFirstJob(streamingAppTracker: StreamingAppTracker,
                          batchDescription: BatchDescription): Long = {
    streamingAppTracker.batchIdToJobId(batchDescription).min
  }

  private def getLastJob(streamingAppTracker: StreamingAppTracker,
                         batchDescription: BatchDescription): Long = {
    streamingAppTracker.batchIdToJobId(batchDescription).max
  }

  private def filterExecutorIDs(jobIdToExecutorId: mutable.Map[Long, Set[String]],
                                jobIDs: List[Long]): List[String] = {
    val filteredJobIdToExecutorId = jobIdToExecutorId.retain(
      (key, value) => jobIDs.contains(key))
    val executorIDs = filteredJobIdToExecutorId.values.flatten.toList.distinct
    executorIDs
  }
}
