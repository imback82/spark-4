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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

object DynamicBucketRepartitioning extends Rule[LogicalPlan]  {
  private val sqlConf = SQLConf.get

  private def isPlanLinear(plan: LogicalPlan): Boolean = {
    plan.children.length <= 1 && plan.children.forall(isPlanLinear)
  }

  private def getBucketSpec(plan: LogicalPlan): Option[BucketSpec] = {
    if (isPlanLinear(plan)) {
      plan.collectFirst {
        case _ @ LogicalRelation(r: HadoopFsRelation, _, _, _)
          if r.bucketSpec.nonEmpty && r.bucketSpec.get.numParallelism.isEmpty =>
          r.bucketSpec.get
      }
    } else {
      None
    }
  }

  private def getNumberBuckets(left: BucketSpec, right: BucketSpec) : Option[Int] = {
    val resize = sqlConf.getConfString("spark.sql.bucketing.resize", null)
    val applyRepartition = sqlConf.getConfString("spark.sql.bucketing.repartition", null)
    val applyCoalesce = sqlConf.getConfString("spark.sql.bucketing.coalesce", null)

    if (resize != null) {
      Some(resize.toInt)
    } else if (applyRepartition != null) {
      Some(math.max(left.numBuckets, right.numBuckets))
    } else if (applyCoalesce != null) {
      Some(math.min(left.numBuckets, right.numBuckets))
    } else {
      None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val applyBucketing = sqlConf.getConfString("spark.sql.bucketing.enable", "true")
    if (applyBucketing == "false") {
      return plan
    }

    plan transform {
      case join : Join =>
        val leftBucket = getBucketSpec(join.left)
        val rightBucket = getBucketSpec(join.right)
        if (leftBucket.isEmpty || rightBucket.isEmpty) {
          return plan
        }

        val newNumBuckets = getNumberBuckets(leftBucket.get, rightBucket.get)
        if (newNumBuckets.isEmpty) {
          return plan
        }

        join.transformUp {
          case l @ LogicalRelation(r: HadoopFsRelation, _, _, _)
            if r.bucketSpec.nonEmpty && r.bucketSpec.get.numBuckets != newNumBuckets.get =>
            val newBucketSpec = r.bucketSpec.get.copy(numParallelism = newNumBuckets)
            val newRelation: HadoopFsRelation =
              r.copy(bucketSpec = Some(newBucketSpec))(r.sparkSession)
            l.copy(relation = newRelation)
        }
      case other => other
    }
  }
}
