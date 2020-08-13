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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch

private[spark] class SplitBucketRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient override val filePartitions: Seq[FilePartition],
    bucketSpec: BucketSpec,
    newNumBuckets: Int,
    output: Seq[Attribute])
  extends FileScanRDD(sparkSession, readFunction, filePartitions) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter: Iterator[_] = super.compute(split, context)
    iter.map {
      case batch: ColumnarBatch =>
        batch.rowIterator().next()
      case other => other
    }.filter {
      case r: InternalRow =>
        getBucketId(r) == split.index
      case _ => false
    }.asInstanceOf[Iterator[InternalRow]]
  }

  private lazy val getBucketId: InternalRow => Int = {
    val bucketIdExpression = {
      val bucketColumns = bucketSpec.bucketColumnNames.map(c => output.find(_.name == c).get)
      HashPartitioning(bucketColumns, newNumBuckets).partitionIdExpression
    }

    val projection = UnsafeProjection.create(Seq(bucketIdExpression), output)
    row => projection(row).getInt(0)
  }
}
