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

import com.qubole.spark.streaminglens.StreamingLens
import com.qubole.spark.streaminglens.common.BatchDescription
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, JobTimeSpan, StageTimeSpan}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession


class StreamingAppListener(sparkSession: SparkSession, streamingLens: StreamingLens)
  extends SparkListener with Logging {

  private val QUERY_ID_KEY = "sql.streaming.queryId"
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val defaultExecutorCores = sparkSession.sparkContext.getConf.getInt(
    "spark.executor.cores", 2)

  private lazy val streamingAppTracker = streamingLens.getStreamingAppTracker()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    execute("onJobStart", () => {
      val jobTimeSpan = new JobTimeSpan(jobStart.jobId)
      jobTimeSpan.setStartTime(jobStart.time)
      streamingAppTracker.jobMap.put(jobStart.jobId, jobTimeSpan).foreach( _ =>
        logInfo(s"Already found entry for Job Id ${jobStart.jobId} in job map"))
      jobStart.stageIds.foreach( stageID => {
        streamingAppTracker.stageIDToJobID.put(stageID, jobStart.jobId).foreach( _ =>
          logInfo(s"Already found entry for Stage Id $stageID in stageIDToJobID map"))
      })
      val sqlExecutionID = jobStart.properties.getProperty("spark.sql.execution.id")
      if (sqlExecutionID != null && !sqlExecutionID.isEmpty) {
        streamingAppTracker.jobSQLExecIDMap.put(jobStart.jobId, sqlExecutionID.toLong).foreach( _ =>
          logInfo(s"Already found entry for Job Id ${jobStart.jobId} in jobSQLExecIDMap map"))
        logDebug(s"sqlExecutionID: ${sqlExecutionID.toString}")
      } else {
        logInfo(s"sqlExecutionID not found")
      }
      if (jobStart.properties.getProperty(QUERY_ID_KEY) != null) {
        val batchDescriptionString = jobStart.properties.getProperty(SPARK_JOB_DESCRIPTION)
        val batchDescription = BatchDescription.parseBatchDescriptionString(batchDescriptionString)
        if (streamingAppTracker.batchIdToJobId.contains(batchDescription)) {
          streamingAppTracker.batchIdToJobId(batchDescription) =
            streamingAppTracker.batchIdToJobId(batchDescription) :+ jobStart.jobId.toLong
        } else {
          streamingAppTracker.batchIdToJobId(batchDescription) = List(jobStart.jobId.toLong)
        }
      }

    })
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    execute("onJobEnd", () => {
      val jobTimeSpan = streamingAppTracker.jobMap.get(jobEnd.jobId)
      jobTimeSpan match {
        case Some(timeSpan) =>
          timeSpan.setEndTime(jobEnd.time)
        case None =>
          logInfo(s"Job Id ${jobEnd.jobId} not found in Job Map")
      }
    })
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    execute("onTaskStart", () => {
      val taskInfo = taskStart.taskInfo
      val executorTimeSpan = streamingAppTracker.executorMap.get(taskInfo.executorId)
      if (executorTimeSpan.isEmpty) {
        val timeSpan = new ExecutorTimeSpan(taskInfo.executorId,
          taskInfo.host, defaultExecutorCores)
        timeSpan.setStartTime(taskInfo.launchTime)
        streamingAppTracker.executorMap(taskInfo.executorId) = timeSpan
      }
      val jobIdOption = streamingAppTracker.stageIDToJobID.get(taskStart.stageId)
      if (jobIdOption.isEmpty) {
        logInfo(s"Stage Id ${taskStart.stageId} not found in stageIDToJobID")
      }
      jobIdOption.foreach { jobId =>
        if (streamingAppTracker.jobIdToExecutorId.contains(jobId)) {
          if (!streamingAppTracker.jobIdToExecutorId(jobId).contains(taskInfo.executorId)) {
            streamingAppTracker.jobIdToExecutorId(jobId) =
              streamingAppTracker.jobIdToExecutorId(jobId) + taskInfo.executorId
          }
        } else {
          streamingAppTracker.jobIdToExecutorId(jobId) = Set(taskInfo.executorId)
        }
      }
    })
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    execute("onTaskEnd", () => {
      val taskMetrics = taskEnd.taskMetrics
      val taskInfo = taskEnd.taskInfo

      if (taskMetrics == null) return

      val executorTimeSpan = streamingAppTracker.executorMap.get(taskInfo.executorId)
      if (executorTimeSpan.isDefined) {
        // update the executor metrics
        executorTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
      }

      val stageTimeSpan = streamingAppTracker.stageMap.get(taskEnd.stageId)
      if (stageTimeSpan.isDefined) {
        // update stage metrics
        stageTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
        stageTimeSpan.get.updateTasks(taskInfo, taskMetrics)
      }
      val jobID = streamingAppTracker.stageIDToJobID.get(taskEnd.stageId)
      if (jobID.isDefined) {
        val jobTimeSpan = streamingAppTracker.jobMap.get(jobID.get)
        if (jobTimeSpan.isDefined) {
          // update job metrics
          jobTimeSpan.get.updateAggregateTaskMetrics(taskMetrics, taskInfo)
        }
      }
      if (taskEnd.taskInfo.failed) {
        logWarning(s"\nTask Failed \n ${taskEnd.reason}")
      }

    })
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    execute("onStageSubmitted", () => {
      if (streamingAppTracker.stageMap.get(stageSubmitted.stageInfo.stageId).isEmpty) {
        val stageTimeSpan = new StageTimeSpan(stageSubmitted.stageInfo.stageId,
          stageSubmitted.stageInfo.numTasks)
        stageTimeSpan.setParentStageIDs(stageSubmitted.stageInfo.parentIds)
        if (stageSubmitted.stageInfo.submissionTime.isDefined) {
          stageTimeSpan.setStartTime(stageSubmitted.stageInfo.submissionTime.get)
        }
        streamingAppTracker.stageMap.put(stageSubmitted.stageInfo.stageId, stageTimeSpan)
      }
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    execute("onStageCompleted", () => {
      val stageTimeSpan = streamingAppTracker.stageMap.get(stageCompleted.stageInfo.stageId)
      stageTimeSpan match {
        case None =>
          logInfo(s"Stage Id ${stageCompleted.stageInfo.stageId} not found in Stage Map")
        case Some(timeSpan) =>
          if (stageCompleted.stageInfo.completionTime.isDefined) {
            timeSpan.setEndTime(stageCompleted.stageInfo.completionTime.get)
          }
          if (stageCompleted.stageInfo.submissionTime.isDefined) {
            timeSpan.setStartTime(stageCompleted.stageInfo.submissionTime.get)
          }
          if (stageCompleted.stageInfo.failureReason.isDefined) {
            // stage failed
            timeSpan.finalUpdate()
          } else {
            val jobIDOption = streamingAppTracker.stageIDToJobID.get(
              stageCompleted.stageInfo.stageId)
            jobIDOption match {
              case None =>
                logInfo(s"Stage Id ${stageCompleted.stageInfo.stageId} " +
                  s"not found in stageIDToJobID Map")
              case Some(jobID) =>
                val jobTimeSpan = streamingAppTracker.jobMap.get(jobID)
                if (jobTimeSpan.isEmpty) {
                  logInfo(s"Job Id ${jobID} not found in Job Map")
                }
                jobTimeSpan.foreach(_.addStage(timeSpan))
            }
            timeSpan.finalUpdate()
          }
      }
    })
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    execute("onExecutorAdded", () => {
      val executorTimeSpan = streamingAppTracker.executorMap.get(executorAdded.executorId)
      if (executorTimeSpan.isEmpty) {
        val timeSpan = new ExecutorTimeSpan(executorAdded.executorId,
          executorAdded.executorInfo.executorHost,
          executorAdded.executorInfo.totalCores)
        timeSpan.setStartTime(executorAdded.time)
        streamingAppTracker.executorMap.put(executorAdded.executorId, timeSpan)
      }
    })
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    execute( "onExecutorRemoved", () => {
      val executorTimeSpan = streamingAppTracker.executorMap.get(executorRemoved.executorId)
      executorTimeSpan match {
        case Some(timeSpan) =>
          timeSpan.setEndTime(executorRemoved.time)
        case None =>
          logInfo(s"Executor Id ${executorRemoved.executorId} not found in Executor Map")
      }
    })
  }

  private def execute(name: String, func: () => Unit): Unit = {
    try {
      func()
    } catch {
      case e: Exception =>
        logWarning(s"Error occurred in ${name}. Streaming Insights may not be reliable"
          + e.getMessage)
    }
  }

}