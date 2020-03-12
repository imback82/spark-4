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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

object BucketingCoalesce extends Rule[LogicalPlan]  {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val bucketingRepartition = plan collectFirst {
      case b : BucketingRepartition => b
    }
    if (bucketingRepartition.nonEmpty) {
      return plan
    }

    val bucketSpecs = plan collect {
      case _ @ LogicalRelation(r: HadoopFsRelation, _, _, _) if r.bucketSpec.nonEmpty =>
        r.bucketSpec.get
    }

    if (bucketSpecs.isEmpty) {
      return plan
    }

    val minBuckets = bucketSpecs.map(_.numBuckets).min
    plan transformUp {
      case l @ LogicalRelation(r: HadoopFsRelation, _, _, _)
          if r.bucketSpec.nonEmpty && r.bucketSpec.get.numBuckets != minBuckets =>
        BucketingRepartition(minBuckets, r.bucketSpec.get, l)
    }
  }
}
