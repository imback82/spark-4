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
import scala.reflect.runtime.universe._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{CoalescedRDD, CoalescedRDDPartition, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.vectorized.{MappedColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

private[spark] class BucketRepartitionRDD2(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient override val filePartitions: Seq[FilePartition],
    bucketSpec: BucketSpec,
    output: Seq[Attribute])
  extends FileScanRDD(sparkSession, readFunction, filePartitions) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    def getIter[A: TypeTag](iter: Iterator[A]): Iterator[_] = iter match {
      case _ if typeOf[A] =:= typeOf[ColumnarBatch] =>
        iter.map {
          case batch: ColumnarBatch =>
            val mapping = new ListBuffer[Int]()
            for (i <- 0 until batch.numRows) {
              if (getBucketId(batch.getRow(i)) == split.index) {
                mapping.append(i)
              }
            }
            for (i <- 0 until batch.numCols) {
              convert(mapping, batch.column(i).asInstanceOf[WritableColumnVector])
            }
            batch.setNumRows(mapping.length)
            batch
        }
      case _ if typeOf[A] =:= typeOf[InternalRow] =>
        iter.filter {
          case r: InternalRow =>
            getBucketId(r) == split.index
        }
    }
    getIter(super.compute(split, context)).asInstanceOf[Iterator[InternalRow]]
  }

  private def convert(mapping: Seq[Int], col: WritableColumnVector): Unit = {
    col.dataType match {
      case _: BooleanType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putBoolean(i, col.getBoolean(mapping(i)))
          }
        }
      case _: ByteType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putByte(i, col.getByte(mapping(i)))
          }
        }
      case _: ShortType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putShort(i, col.getShort(mapping(i)))
          }
        }
      case _: IntegerType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putInt(i, col.getInt(mapping(i)))
          }
        }
      case _: LongType =>
        for (i <- 0 until mapping.length) {
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
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putDouble(i, col.getDouble(mapping(i)))
          }
        }
      case _: StringType =>
        for (i <- 0 until mapping.length) {
          if (col.isNullAt(mapping(i))) {
            col.putNull(i)
          } else {
            col.putArray(i, col.getArrayOffset(mapping(i)), col.getArrayLength(mapping(i)))
          }
        }
    }
  }
//
//      else if (dt.isInstanceOf[BinaryType]) row.update(i, getBinary(i))
//      else if (dt.isInstanceOf[DecimalType]) {
//        val t = dt.asInstanceOf[DecimalType]
//        row.setDecimal(i, getDecimal(i, t.precision, t.scale), t.precision)
//      }
//      else if (dt.isInstanceOf[DateType]) row.setInt(i, getInt(i))
//      else if (dt.isInstanceOf[TimestampType]) row.setLong(i, getLong(i))
//      else throw new RuntimeException("Not implemented. " + dt)
//    }
//  }

  private lazy val getBucketId: InternalRow => Int = {
    val bucketIdExpression = {
      val bucketColumns = bucketSpec.bucketColumnNames.map(
        c => output.find(_.name == c).get)
      HashPartitioning(bucketColumns, bucketSpec.numParallelism.get).partitionIdExpression
    }

    val projection = UnsafeProjection.create(Seq(bucketIdExpression), output)
    row => projection(row).getInt(0)
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
