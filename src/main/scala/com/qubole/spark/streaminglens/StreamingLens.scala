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

import com.qubole.spark.streaminglens.config.StreamingLensConfig
import com.qubole.spark.streaminglens.listener.{QueryProgressListener, StreamingAppListener}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class StreamingLens(sparkSession: SparkSession,
                    options: Map[String, String]) extends Logging {

  def this(sparkSession: SparkSession, reporterClassName: String) = this(
    sparkSession,
    Map("streamingLens.reporter.className" -> reporterClassName,
      "streamingLens.reporter.enabled" -> "true"))

  def this(sparkSession: SparkSession, expectedMicroBatchSLA: Long) = this(
    sparkSession,
    Map("streamingLens.expectedMicroBatchSLAMillis" -> expectedMicroBatchSLA.toString))

  def this(sparkSession: SparkSession, expectedMicroBatchSLA: Long, reporterClassName: String) =
    this(
      sparkSession,
      Map(
        "streamingLens.reporter.className" -> reporterClassName,
        "streamingLens.reporter.enabled" -> "true",
        "streamingLens.expectedMicroBatchSLAMillis" -> expectedMicroBatchSLA.toString))

  private val streamingAppTracker = new StreamingAppTracker()

  private val streamingLensConfig = new StreamingLensConfig(options)

  private val streamingAppListener = new StreamingAppListener(sparkSession, this)

  private val queryProgressListener = new QueryProgressListener(
    this, streamingLensConfig)

  registerListeners()

  def registerListeners(): Unit = {

    try {
      sparkSession.sparkContext.addSparkListener(streamingAppListener)
      logDebug("Successfully registered Spark Listener")
    } catch {
      case e: Exception =>
        throw new SparkException("Error in registering Spark Listener " +
          "Won't report StreamingLens Insights" + e.getMessage)
    }
    try {
      sparkSession.streams.addListener(queryProgressListener)
      logDebug("Successfully registered StreamingQuery Listener")
    } catch {
      case e: Exception =>
        sparkSession.sparkContext.removeSparkListener(streamingAppListener)
        throw new SparkException("Error in registering StreamingQuery Listener " +
          "Won't report StreamingLens Insights" + e.getMessage)
    }

  }

  def updateExpectedMicroBatchSLA(queryDesc: String, sla: Long): Unit = {
    streamingAppTracker.setExpectedMicroBatchSLA(queryDesc, sla)
  }

  def resetExpectedMicroBatchSLA(queryDesc: String): Unit = {
    streamingAppTracker.removeExpectedMicroBatchSLA(queryDesc)
  }

  def removeListeners(): Unit = {
    try {
      sparkSession.sparkContext.removeSparkListener(streamingAppListener)
    } catch {
      case e: Exception =>
        logWarning("Error in removing Spark Listener" + e. getMessage)
    }
    try {
      sparkSession.streams.removeListener(queryProgressListener)
    } catch {
      case e: Exception =>
        logWarning("Error in removing StreamingQuery Listeners" + e.getMessage)
    }

  }

  def stopStreamingLens(): Unit = {
    removeListeners()
    streamingAppTracker.stopStreamingAppTracker()
  }

  def getStreamingAppTracker(): StreamingAppTracker = {
    streamingAppTracker
  }


}
