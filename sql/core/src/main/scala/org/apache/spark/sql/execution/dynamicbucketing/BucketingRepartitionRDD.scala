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

import scala.collection.mutable.ListBuffer

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch

private[spark] class BucketingRepartitionRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient override val filePartitions: Seq[FilePartition],
    bucketSpec: BucketSpec,
    output: Seq[Attribute])
  extends FileScanRDD(sparkSession, readFunction, filePartitions) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter: Iterator[_] = super.compute(split, context)
    iter.map {
      case batch: ColumnarBatch =>
        var curIndex = 0
        val mapping = new Array[Int](batch.numRows)
        for (i <- 0 until batch.numRows) {
          if (getBucketId(batch.getRow(i)) == split.index) {
            mapping(curIndex) = i
            curIndex += 1
          }
        }
        for (i <- 0 until batch.numCols) {
          convert(mapping, curIndex, batch.column(i).asInstanceOf[WritableColumnVector])
        }
        batch.setNumRows(mapping.length)
        batch
      case other => other
    }
    .filter {
      case r: InternalRow =>
        getBucketId(r) == split.index
      case _ => true
    }.asInstanceOf[Iterator[InternalRow]]
  }

  private lazy val getBucketId: InternalRow => Int = {
    val bucketIdExpression = {
      val bucketColumns = bucketSpec.bucketColumnNames.map(
        c => output.find(_.name == c).get)
      HashPartitioning(bucketColumns, bucketSpec.numParallelism.get).partitionIdExpression
    }

    val projection = UnsafeProjection.create(Seq(bucketIdExpression), output)
    row => projection(row).getInt(0)
  }

  private def convert(mapping: Array[Int], mappingLength: Int, col: WritableColumnVector): Unit = {
    col.dataType match {
      case _: BooleanType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putBoolean(i, col.getBoolean(mapping(i)))
          }
        }
      case _: ByteType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putByte(i, col.getByte(mapping(i)))
          }
        }
      case _: ShortType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putShort(i, col.getShort(mapping(i)))
          }
        }
      case _: IntegerType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putInt(i, col.getInt(mapping(i)))
          }
        }
      case _: LongType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putLong(i, col.getLong(mapping(i)))
          }
        }
      case _: FloatType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putFloat(i, col.getFloat(mapping(i)))
          }
        }
      case _: DoubleType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putDouble(i, col.getDouble(mapping(i)))
          }
        }
      case _: StringType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putArray(i, col.getArrayOffset(mapping(i)), col.getArrayLength(mapping(i)))
          }
        }
      case _: BinaryType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putArray(i, col.getArrayOffset(mapping(i)), col.getArrayLength(mapping(i)))
          }
        }
      case dt: DecimalType =>
        if (dt.precision <= Decimal.MAX_INT_DIGITS) {
          for (i <- 0 until mappingLength) {
            if (col.isNullAt(mapping(i))) {
              col.putNull(i)
            } else {
              col.putInt(i, col.getInt(mapping(i)))
            }
          }
        } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
          for (i <- 0 until mappingLength) {
            if (col.isNullAt(mapping(i))) {
              col.putNull(i)
            } else {
              col.putLong(i, col.getLong(mapping(i)))
            }
          }
        } else {
          for (i <- 0 until mappingLength) {
            if (col.isNullAt(mapping(i))) {
              col.putNull(i)
            } else {
              col.putArray(i, col.getArrayOffset(mapping(i)), col.getArrayLength(mapping(i)))
            }
          }
        }
      case _: CalendarIntervalType =>
        val months = col.getChild(0)
        val microseconds = col.getChild(1)
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            months.putInt(i, months.getInt(mapping(i)))
            microseconds.putLong(i, microseconds.getLong(mapping(i)))
          }
        }
      case _: DateType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putInt(i, col.getInt(mapping(i)))
          }
        }
      case _: TimestampType =>
        for (i <- 0 until mappingLength) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putLong(i, col.getLong(mapping(i)))
          }
        }
    }
  }
}
