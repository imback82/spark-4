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

import org.apache.spark.Partition
import org.apache.spark.rdd.{CoalescedRDD, CoalescedRDDPartition, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

private[spark] class BucketingCoalescedRDD(
    prev: RDD[InternalRow],
    numBuckets: Int) extends CoalescedRDD[InternalRow](prev, numBuckets) {
  override def getPartitions: Array[Partition] = {
    prev.partitions.groupBy(p => p.index % numBuckets).toSeq.sortBy(_._1).map { x =>
      CoalescedRDDPartition(x._1, prev, x._2.map(_.index))
    }.toArray
  }
}

case class BucketingCoalesceExec(
    numBuckets: Int,
    originalBucketSpec: BucketSpec,
    child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case h: HashPartitioning => HashPartitioning(h.expressions, numBuckets)
      case other => other
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    new BucketingCoalescedRDD(child.execute(), numBuckets)
  }
}
