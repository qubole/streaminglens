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

package com.qubole.spark.streaminglens.config

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class StreamingLensConfig(parameters: CaseInsensitiveMap[String]) extends Logging {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val analysisIntervalMinutes: Int = withIntParameter("streamingLens.analysisIntervalMinutes", 5)

  val laggingThreshold: Double = withDoubleParameter(
    "streamingLens.criticalPathAnalysis.overProvisionPercentage", 0.3)

  val okayThreshold: Double = withDoubleParameter(
    "streamingLens.criticalPathAnalysis.underProvisionPercentage", 0.7)

  val criticalPathThreshold: Double = withDoubleParameter(
    "streamingLens.criticalPathAnalysis.criticalPathPercentage", 0.7)

  val minBatches: Int = withIntParameter("streamingLens.minBatches", 1)

  val maxResultsRetention: Int = withIntParameter("streamingLens.maxResultsRetention", 30)

  val maxBatchesRetention: Int = withIntParameter("streamingLens.maxBatchesRetention", 10)

  val maxAnalysisTimeSeconds: Int = withIntParameter("streamingLens.maxAnalysisTimeSeconds", 5)

  val maxRetries: Int = withIntParameter("streamingLens.maxRetries", 3)

  val shouldLogResults: Boolean = withBooleanParameter("streamingLens.shouldLogResults", true)

  val enableReporting: Boolean = withBooleanParameter("streamingLens.reporter.enabled", false)

  val expectedMicroBatchSLAMillis: Long = withLongParameter(
    "streamingLens.expectedMicroBatchSLAMillis", 1000 * 60 * 2)

  val reporterClassName: String = parameters.getOrElse(
    "streamingLens.reporter.className", "JsonFileReporter")

  val discountFactor: Double = withDoubleParameter("streamingLens.reporter.discountFactor", 0.95)

  val reportingIntervalMinutes: Int = withIntParameter("streamingLens.reporter.intervalMinutes", 60)

  val reportingOptions: Map[String, String] = parameters.filterKeys(
    _.contains("streamingLens.reporter"))

  private def withDoubleParameter(name: String, default: Double): Double = {
    parameters.get(name).map { str =>
      Try(str.toDouble).toOption.filter(x => x > 0 && x < 1).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '$name', must be between 0 and 1")
      }
    }.getOrElse(default)
  }

  private def withLongParameter(name: String, default: Int): Long = {
    parameters.get(name).map { str =>
      Try(str.toLong).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '$name', must be a positive integer")
      }
    }.getOrElse(default)
  }

  private def withIntParameter(name: String, default: Int): Int = {
    parameters.get(name).map { str =>
      Try(str.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '$name', must be a positive integer")
      }
    }.getOrElse(default)
  }


  private def withBooleanParameter(name: String, default: Boolean) = {
    parameters.get(name).map { str =>
      try {
        str.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option '$name', must be true or false")
      }
    }.getOrElse(default)
  }


}
