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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

case class MappedColumnVector(wrapped: ColumnVector, mapping: IndexedSeq[Int])
    extends ColumnVector(wrapped.dataType) {
  override def close(): Unit = wrapped.close

  override def hasNull: Boolean = wrapped.hasNull

  override def numNulls(): Int = wrapped.numNulls

  override def isNullAt(rowId: Int): Boolean = wrapped.isNullAt(mapping(rowId))

  override def getBoolean(rowId: Int): Boolean = wrapped.getBoolean(mapping(rowId))

  override def getByte(rowId: Int): Byte = wrapped.getByte(mapping(rowId))

  override def getShort(rowId: Int): Short = wrapped.getShort(mapping(rowId))

  override def getInt(rowId: Int): Int = wrapped.getInt(mapping(rowId))

  override def getLong(rowId: Int): Long = wrapped.getLong(mapping(rowId))

  override def getFloat(rowId: Int): Float = wrapped.getFloat(mapping(rowId))

  override def getDouble(rowId: Int): Double = wrapped.getDouble(mapping(rowId))

  override def getArray(rowId: Int): ColumnarArray = wrapped.getArray(mapping(rowId))

  override def getMap(rowId: Int): ColumnarMap = wrapped.getMap(mapping(rowId))

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    wrapped.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = wrapped.getUTF8String(mapping(rowId))

  override def getBinary(rowId: Int): Array[Byte] = wrapped.getBinary(mapping(rowId))

  override protected def getChild(ordinal: Int): ColumnVector =
    MappedColumnVector(wrapped.getChild(ordinal), mapping)
}
