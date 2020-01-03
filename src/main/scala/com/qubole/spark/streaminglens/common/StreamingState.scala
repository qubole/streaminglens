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

object StreamingState extends Enumeration {

  type Mode = Value
  val UNHEALTHY = Value(4)
  val UNDERPROVISIONED = Value(3)
  val OPTIMUM = Value(2)
  val OVERPROVISIONED = Value(1)
  val NONEWBATCHES = Value(0)
  val ERROR = Value(-1)
}
