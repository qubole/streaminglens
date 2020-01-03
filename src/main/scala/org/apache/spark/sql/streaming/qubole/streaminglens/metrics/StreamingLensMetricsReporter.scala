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


package org.apache.spark.sql.streaming.qubole.streaminglens.metrics

import scala.collection.mutable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.{Source => CodahaleSource}
import com.qubole.spark.streaminglens.common.StreamingState
import com.qubole.spark.streaminglens.common.results.StreamingLensResults


class StreamingLensMetricsReporter(queryDesc: String,
                                   streamingLensResults: mutable.Queue[StreamingLensResults])
  extends CodahaleSource {

  override val sourceName = s"spark.streaming.$queryDesc"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  registerMetrics()
  registerSource()

  private def registerMetrics(): Unit = {

    registerGauge(
      "expectedMicroBatchSLAMillis", _.streamingCriticalPathResults.expectedMicroBatchSLA, 0L)
    registerGauge("batchRunningTimeMillis", _.streamingCriticalPathResults.batchRunningTime, 0L)
    registerGauge("criticalTimeMillis", _.streamingCriticalPathResults.criticalTime, 0L)
    registerGauge("streamingQueryState",
      _.streamingCriticalPathResults.streamingQueryState.id, StreamingState.NONEWBATCHES.id)
    registerGauge("analysisTimeMillis", _.analysisTime, 0L)

  }

  private def registerSource(): Unit = {
    SparkEnv.get.metricsSystem.registerSource(this)
  }

  def removeSource(): Unit = {
    SparkEnv.get.metricsSystem.removeSource(this)
  }

  private def registerGauge[T](name: String,
                               f: StreamingLensResults => T,
                               default: T): Unit = {
    synchronized {
      metricRegistry.register(name, new Gauge[T] {
        override def getValue: T = Option(
          streamingLensResults.lastOption.orNull).map(f).getOrElse(default)
      })
    }
  }

}