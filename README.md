# Streaminglens

Streaminglens is a profiling tool for Spark Structured Streaming Applications running in micro-batch mode. Since the execution plan for each micro-batch is identical, we can continuously learn from previous micro-batches to predict the ideal spark cluster configurations for the next micro-batch. Streaminglens analyzes the excecution run of last micro-batch every five minutes to give an overall idea of the health of the streaming pipeline.

During this analysis, Streaminglens calculates the critical time to complete a micro-batch. Critical Time is the minimum time a Spark job would take to complete if it is run with infinite executors. For more information on how critical time is calculated visit our blog on [Spark tunig tool](https://www.qubole.com/blog/introducing-quboles-spark-tuning-tool/). Streaminglens also takes expected micro-batch SLA as input and it expects every micro-batch to complete before the specified SLA.

Based on the comparison of critical time, actual batch running time and expected microbatch SLA, streaminglens decides the state of the streaming pipeline as `Optimum`, `Underprovisioned`, `Overprovisioned` or `Unhealthy` and gives appropiate recommendations to tune the spark cluster.

## Streaming Pipeline States

State | Description
--- | ---
No New Batches | No new data in the pipeline. If this state persists despite data ingestion, some batch may be stuck, check Spark UI
Overprovisioned | Batch completion time is far less than expected SLA, so you can downscale cluster to reduce costs. On the other hand, if yo see this state but the stream is lagging, lower Trigger Interval or check for specific recommendations.
Optimum | Streaming Pipeline is meeting SLA comfortably.
Underprovisioned | You need to Upscale Cluster to match Expected Microbatch SLA
Unhealthy | You need to increase ingestion at Source level by altering spark cluster or pipeline configurations; Check for Specific Recommendations

## How are insights reported?

Streaminglens reports insights through following ways:
* Insights are printed in Driver logs of Spark Application
* Streaminglens publishes its metrics through Dropwizard. You can view these metrics through any of the supported metrics sinks in Apache Spark. For more details on various metrics sinks, visit [Apache Spark documentation](https://spark.apache.org/docs/latest/monitoring.html#metrics).
* You can use your own custom reporter to see the aggregated health of the streaming pipeline and recommendations every one hour. For more details see the [Using Custom Reporters section](#3-Using-custom-reporters).


## What insights can you expect?

Streaminglens reports following insights after analyzing the execution of last micro-batch:
* Comparison of Batch Running Time, Critical Time & Trigger Interval
* State of streaming pipeline as `Underprovisioned`, `Optimum`, `Overprovisioned` or `Unhealthy`.
* Aggregated state of the streaming pipeline every one hour along with recommendations in case the pipeline is not optimum.

A sample insight printed in driver log is shown below:

```
|||||||||||||||||| StreamingLens Inisights |||||||||||||||||||||||||
BatchId: 8
Analysis Time: 00s 013ms
Expected Micro Batch SLA: 10s 000ms
Batch Running Time: 02s 094ms
Critical Time: 02s 047ms
Streaming Query State: OVERPROVISIONED
||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
```

An example of aggregated state and recommendations: 

```
Streaming Query State: OVERPROVISIONED
Recommendations:
~ Tune source configurations to ingest more data
~ Decrease the value of trigger Interval to process latest data
~ You can decrease the number of executors to reduce cost
```

## How to build Streaminglens?

Streaminglens is built using Apache Maven](http://maven.apache.org/).

To build Streaminglens, clone this repository and run:
```
mvn -DskipTests clean package
```

This will create `target/spark-streaminglens_2.11-0.1.0.jar` file which contains streaminglens code and associated dependencies. Make sure the Scala and Java versions correspond to those required by your Spark cluster. We have tested it with Java 7/8, Scala 2.11 and Spark version 2.4.0. 

## How to use Streaminglens?

#### 1. Adding streaminglens jar to spark application

Once you have the streaminglens jar avaialbale, you need to add it to your spark-submit command line options
```
--jars /path/to/spark-streaminglens_2.11-0.1.0.jar
```

You could also add this to your cluster's `spark-defaults.conf` so that it is automatically
avaialable for all applications.

#### 2. Initializing streaminglens

You need to initialize streaminglens by adding following line to your code
```
import com.qubole.spark.StreamingLens

val streamingLens = new StreamingLens(sparkSession, options)
```

Streaminglens requires spark session object and a Map of type [String, String] to be passed as parameters. For more details about various options see the [Configuring Streaminglens Options](#4-configuring-streaminglens-options) section below.

#### 3. Using custom reporters

You can use your own custom reporter to report aggregated health of streaming pipeline and 
assoicated recommendations. You just need to extend `StreamingLensEventsReporterInterface` and pass
the class name with `streamingLens.reporter.className` option and set 
`streamingLens.reporter.enabled` to true. 

You can also configure the reporting frequecy through `streamingLens.reporter.intervalMinutes` 
option. By default, aggregated health and recommendations are reported every one hour. 

If you need to pass any additional options to your StreamingLens reporter class, prefix them with `streamingLens.reporter` and pass them along with other streaminglens parameters.

#### 4. Configuring Streaminglens Options

Streaminglens supports various configuration options.

Name |Default | Meaning
--- |:---:| ---
`streamingLens.analysisIntervalMinutes`|5 mins|Frequency of analysis of micro-batches
`streamingLens.criticalPathAnalysis.overProvisionPercentage`|0.3 (or 30%)|Percentage below which to consider spark cluster as over-provisoned, example: if batch running time is less than 30% of expected micro-batch SLA, cluster is considered over-provisioned
`streamingLens.criticalPathAnalysis.underProvisionPercentage`|0.7 (or 70%)|Percentage above which to consider spark cluster as under-provisioned, example: if batch running time is more than 70% of expected micro-batch SLA, cluster is considered under-provisioned
`streamingLens.criticalPathAnalysis.criticalPathPercentage`|0.7 (or 70%)|Percentage above which to consider spark application configured incorrectly, example: if critical time is more than 70% of expected micro-batch SLA, pipeline is unhealthy and spark cluster is improperly configured
`streamingLens.minBatches`|1|Minimum no of batches which must be completed before doing next analysis
`streamingLens.maxResultsRetention`|30|Number of analysis results to retain in-memory
`streamingLens.maxBatchesRetention`|10|Number of batches for which metrics are retained in-memory
`streamingLens.maxAnalysisTimeSeconds`|5|Number of seconds to wait before timeout analysis
`streamingLens.maxRetries`|3|Number of retries in case some error occurs during analysis
`streamingLens.shouldLogResults`|true|Whether to print analysis results in spark driver logs
`streamingLens.reporter.enabled`|false|Whether to dump analysis results in any custom output
`streamingLens.expectedMicroBatchSLAMillis`|1000 * 60 * 2|Interval in milliseconds for SLA
`streamingLens.reporter.className`||Fully resolved classname for reporter class
`streamingLens.reporter.discountFactor`|0.95|Exponential factor by which to discount earlier microbatches while computing aggregated state
`streamingLens.reporter.intervalMinutes`|60|Frequency of reporting the health of streaming query

#### 5. Setting expected micro-batch SLA

You can set expected micro-batch SLA (in milliseconds) for a particular streaming query or for the whole spark streaming application. To set expected micro-batch SLA for whole application, pass it in the options map with key `streamingLens.expectedMicroBatchSLAMillis` while starting Streaminglens.

To set expected micro-batch SLA for a streaming query, use the below StreamingLens API
```
streaminglens.updateExpectedMicroBatchSLA(queryName: String, sla: Long)
```

If expected micro-batch SLA is not set, default value of `streamingLens.expectedMicroBatchSLAMillis` is used for all the streaming queries.

#### 6. Stopping Streaminglens 

You can stop streaminglens in a running spark application using following Streaminglens API:
```
streamingLens.stop()
```

## Acknowledgement
Streaminglens implementation would not have been possible without referencing the implementation of [Sparklens](http://sparklens.qubole.com).