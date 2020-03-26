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

package org.apache.spark.sql.execution.dynamicbucketing

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{CoalescedRDD, CoalescedRDDPartition, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}

private[spark] class BucketRepartitionRDD2(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient override val filePartitions: Seq[FilePartition])
  extends FileScanRDD(sparkSession, readFunction, filePartitions) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter: Iterator[Object] = super.compute(split, context)
    iter.filter { row =>
      // getBucketId(row) == partition.index
      false
    }.asInstanceOf[Iterator[InternalRow]]
  }
}

private[spark] class BucketRepartitionRDD(
    prev: RDD[InternalRow],
    originalBucketSpec: BucketSpec,
    numBuckets: Int,
    output: Seq[Attribute]) extends CoalescedRDD[InternalRow](prev, numBuckets) {
  override def getPartitions: Array[Partition] = {
    val parts = new Array[CoalescedRDDPartition](numBuckets)
    for (i <- 0 until numBuckets) {
      parts(i) = CoalescedRDDPartition(i, prev, Array(i % prev.partitions.length))
    }
    parts.toArray
  }

  private lazy val getBucketId: InternalRow => Int = {
    val bucketIdExpression = {
      val bucketColumns = originalBucketSpec.bucketColumnNames.map(
        c => output.find(_.name == c).get)
      HashPartitioning(bucketColumns, numBuckets).partitionIdExpression
    }

    val projection = UnsafeProjection.create(Seq(bucketIdExpression), output)
    row => projection(row).getInt(0)
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {
    assert(partition.asInstanceOf[CoalescedRDDPartition].parents.length == 1)
    partition.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentPartition =>
      firstParent[InternalRow].iterator(parentPartition, context).filter { row =>
        getBucketId(row) == partition.index
      }
    }
  }
}

case class BucketingRepartitionExec(
    numBuckets: Int,
    originalBucketSpec: BucketSpec,
    child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case h: HashPartitioning => HashPartitioning(h.expressions, numBuckets)
      case other => other
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    new BucketRepartitionRDD(child.execute(), originalBucketSpec, numBuckets, output)
  }
}
