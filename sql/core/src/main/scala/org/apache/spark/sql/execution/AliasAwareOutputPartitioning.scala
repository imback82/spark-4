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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning`
 * that satisfies output distribution requirements.
 */
trait AliasAwareOutputPartitioning extends UnaryExecNode {
  protected def outputExpressions: Seq[NamedExpression]

  final override def outputPartitioning: Partitioning = {
    if (!hasAlias(outputExpressions)) {
      child.outputPartitioning match {
        case HashPartitioning(expressions, numPartitions) =>
          val newExpressions = expressions.map {
            case a: AttributeReference =>
              replaceAlias(outputExpressions, a).getOrElse(a)
            case other => other
          }
          HashPartitioning(newExpressions, numPartitions)
        case other => other
      }
    } else {
      child.outputPartitioning
    }
  }

  private def hasAlias(exprs: Seq[NamedExpression]): Boolean =
    exprs.collectFirst { case _: Alias => true }.isDefined

  // Replaces an alias in the `exprs` that matches the given attribute reference.
  private def replaceAlias(
      exprs: Seq[NamedExpression],
      attr: AttributeReference): Option[Attribute] = {
    exprs.collectFirst {
      case a @ Alias(child: AttributeReference, _) if child.semanticEquals(attr) =>
        a.toAttribute
    }
  }
}
