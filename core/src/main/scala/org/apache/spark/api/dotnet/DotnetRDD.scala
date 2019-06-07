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

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD

object DotnetRDD {
  def createPythonRDD(
      parent: RDD[_],
      func: PythonFunction,
      preservePartitoning: Boolean): PythonRDD = {
    new PythonRDD(parent, func, preservePartitoning)
  }

  def createJavaRDDFromArray(
      sc: SparkContext,
      arr: Array[Array[Byte]],
      numSlices: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(sc.parallelize(arr, numSlices))
  }

  def toJavaRDD(rdd: RDD[_]): JavaRDD[_] = JavaRDD.fromRDD(rdd)
}
