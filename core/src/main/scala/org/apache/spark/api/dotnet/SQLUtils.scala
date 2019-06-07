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

package org.apache.spark.api.dotnet

import java.util.{List => JList, Map => JMap}

import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.DotnetRunner

object SQLUtils {

  /**
   * Exposes createPythonFunction to the .NET client to enable registering UDFs.
   */
  def createPythonFunction(
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      pythonExec: String,
      pythonVersion: String,
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: PythonAccumulatorV2): PythonFunction = {
    // DOTNET_WORKER_SPARK_VERSION is used to handle different versions of Spark on the worker.
    envVars.put("DOTNET_WORKER_SPARK_VERSION", DotnetRunner.SPARK_VERSION)

    PythonFunction(
      command,
      envVars,
      pythonIncludes,
      pythonExec,
      pythonVersion,
      broadcastVars,
      accumulator)
  }
}
