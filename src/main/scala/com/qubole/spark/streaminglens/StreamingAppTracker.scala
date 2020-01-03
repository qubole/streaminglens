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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.qubole.spark.streaminglens.common.BatchDescription
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, JobTimeSpan, StageTimeSpan}

import org.apache.spark.internal.Logging


class StreamingAppTracker extends Logging {

  val jobMap = new ConcurrentHashMap[Long, JobTimeSpan].asScala
  val stageMap = new ConcurrentHashMap[Long, StageTimeSpan].asScala
  val stageIDToJobID = new ConcurrentHashMap[Int, Long].asScala
  val batchIdToJobId = new ConcurrentHashMap[BatchDescription, List[Long]].asScala
  val jobIdToExecutorId = new ConcurrentHashMap[Long, Set[String]].asScala
  val executorMap = new ConcurrentHashMap[String, ExecutorTimeSpan].asScala
  val jobSQLExecIDMap = new ConcurrentHashMap[Long, Long].asScala

  val expectedMicroBatchSLAMap = new ConcurrentHashMap[String, Long].asScala
  val queryInsightsManagerMap = new ConcurrentHashMap[UUID, QueryInsightsManager]().asScala

  def doCleanUp(lastBatchDescription: BatchDescription) : Unit = {

    try {
      val latestJobID = batchIdToJobId(lastBatchDescription).max
      logDebug(s"Latest JOB ID: $latestJobID")
      val earliestJobId = jobMap.keys.min
      logDebug(s"Earliest JOB ID: $earliestJobId")
      val jobIDs: List[Long] = jobMap.filterKeys(key =>
        key >= earliestJobId && key <= latestJobID).keys.toList

      jobIDs.foreach { jobId =>
        logDebug(s"Cleaning up JOB ID: $jobId")
        val jobTimeSpan = jobMap(jobId)
        val stageIDs = jobTimeSpan.stageMap.keys
        stageIDs.foreach { stageID =>
          logDebug(s"Cleaning up STAGE ID: $stageID")
          stageMap.remove(stageID)
          stageIDToJobID.remove(stageID)
          logDebug(s"Successfully cleaned up STAGE ID: $stageID")
        }
        jobMap.remove(jobId)
        jobIdToExecutorId.remove(jobId)
        jobSQLExecIDMap.remove(jobId)
        logDebug(s"Successfully cleaned up JOB ID: $jobId")
      }
    } catch {
      case e: Exception =>
        logWarning("Error occurred while cleaning up data" + e.getMessage)
    }

  }

  def getExpectedMicroBatchSLA(queryDesc: String): Option[Long] = {
    expectedMicroBatchSLAMap.get(queryDesc)
  }

  def setExpectedMicroBatchSLA(queryDesc: String, sla: Long): Unit = {
    if (expectedMicroBatchSLAMap.contains(queryDesc)) {
      logWarning(s"Overriding Micro Batch SLA for query $queryDesc")
    }
    expectedMicroBatchSLAMap.put(queryDesc, sla)
  }

  def removeExpectedMicroBatchSLA(queryDesc: String): Unit = {
    expectedMicroBatchSLAMap.remove(queryDesc).getOrElse {
      logWarning(s"Expected Micro Batch SLA not set for query $queryDesc")
    }
  }

  def stopStreamingAppTracker(): Unit = {
    queryInsightsManagerMap.values.foreach(_.triggerShutDown())
  }


}



