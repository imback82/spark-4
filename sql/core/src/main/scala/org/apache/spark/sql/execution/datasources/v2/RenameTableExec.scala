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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.storage.StorageLevel

/**
 * Physical plan node for renaming a table.
 */
case class RenameTableExec(
    catalog: TableCatalog,
    oldIdent: Identifier,
    newIdent: Identifier,
    invalidateCache: () => Option[(Option[String], StorageLevel)],
    cacheTable: (LogicalPlan, Option[String], StorageLevel) => Unit) extends V2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

    catalog.invalidateTable(oldIdent)
    val optOldCacheInfo = invalidateCache()

    catalog.renameTable(oldIdent, newIdent)

    optOldCacheInfo.foreach { cacheInfo =>
      val tbl = catalog.loadTable(newIdent)
      val newRelation = DataSourceV2Relation.create(tbl, Some(catalog), Some(newIdent))
      val newTableName = s"${catalog.name}.${newIdent.quoted}"
      cacheTable(newRelation, Some(newTableName), cacheInfo._2)
    }
    Seq.empty
  }
}
