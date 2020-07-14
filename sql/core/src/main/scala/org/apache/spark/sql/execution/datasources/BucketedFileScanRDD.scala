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

package org.apache.spark.sql.execution.datasources

import java.io.{FileNotFoundException, IOException}

import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.{Partition => RDDPartition, SparkUpgradeException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, RowOrdering, SortOrder}
import org.apache.spark.sql.execution.{QueryExecutionException, UnsafeExternalRowSorter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{BoundedPriorityQueue, NextIterator}

case class IndexedInternalRow(row: InternalRow, idx: Int)

class IndexedInternalRowOrdering(
    ordering: Ordering[InternalRow]) extends Ordering[IndexedInternalRow] {
  def compare(a: IndexedInternalRow, b: IndexedInternalRow): Int = {
    ordering.compare(a.row, b.row)
  }
}

/**
 * An RDD that scans a list of file partitions.
 */
class BucketedFileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    sortOrder: Seq[SortOrder],
    output: Seq[Attribute])
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      val ordering = RowOrdering.create(sortOrder, output)
      val indexedOrdering = new IndexedInternalRowOrdering(ordering)
      val schema: StructType = StructType.fromAttributes(output)

      val queue: BoundedPriorityQueue[IndexedInternalRow] =
        new BoundedPriorityQueue[IndexedInternalRow](1000)(indexedOrdering)

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      private[this] val fileIterators: IndexedSeq[Iterator[Object]] = getFileIterators.toIndexedSeq

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        queue.nonEmpty || fileIterators.exists(_.hasNext)
        // (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }

      var _nextIdx: Option[Int] = None

      def next(): Object = {
        if (_nextIdx.isDefined) {
          val consumed = fileIterators(_nextIdx.get)
          if (consumed.hasNext) {
            queue += IndexedInternalRow(consumed.next().asInstanceOf[InternalRow], _nextIdx.get)
          }
        }

        if (queue.isEmpty) {
          fileIterators.zipWithIndex.foreach { case(f, i) =>
            if (f.hasNext) {
              val nextElement = f.next()
              if (nextElement.isInstanceOf[ColumnarBatch]) {
                val it = nextElement.asInstanceOf[ColumnarBatch].rowIterator()
                while (it.hasNext) {
                  queue += IndexedInternalRow(it.next, i)
                }
              } else {
                // The following copy is needed for csv, but not for parquet.
                queue += IndexedInternalRow(nextElement.asInstanceOf[InternalRow], i)
              }
            }
          }
        }

        val ret = queue.poll
        _nextIdx = Some(ret.idx)
        ret.row
      }

      def next1(): Object = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        //   don't need to run this `if` for every record.
        val preNumRecordsRead = inputMetrics.recordsRead
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          incTaskInputMetricsBytesRead()
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          // too costly to update every record
          if (inputMetrics.recordsRead %
            SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
            incTaskInputMetricsBytesRead()
          }
          inputMetrics.incRecordsRead(1)
        }
        nextElement
      }

      private def readCurrentFile(): Iterator[InternalRow] = {
        try {
          readFunction(currentFile)
        } catch {
          case e: FileNotFoundException =>
            throw new FileNotFoundException(
              e.getMessage + "\n" +
                "It is possible the underlying files have been updated. " +
                "You can explicitly invalidate the cache in Spark by " +
                "running 'REFRESH TABLE tableName' command in SQL or " +
                "by recreating the Dataset/DataFrame involved.")
        }
      }

      private def readFile(file: PartitionedFile): Iterator[InternalRow] = {
        try {
          readFunction(file)
        } catch {
          case e: FileNotFoundException =>
            throw new FileNotFoundException(
              e.getMessage + "\n" +
                "It is possible the underlying files have been updated. " +
                "You can explicitly invalidate the cache in Spark by " +
                "running 'REFRESH TABLE tableName' command in SQL or " +
                "by recreating the Dataset/DataFrame involved.")
        }
      }

      private def getIterator(file: PartitionedFile): Iterator[Object] = {
        logInfo(s"Reading File $file")

        // TODO: The following needs to be set for the current file be processed.
        // Sets InputFileBlockHolder for the file block's information
        // InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

        if (ignoreMissingFiles || ignoreCorruptFiles) {
          new NextIterator[Object] {
            // The readFunction may read some bytes before consuming the iterator, e.g.,
            // vectorized Parquet reader. Here we use lazy val to delay the creation of
            // iterator so that we will throw exception in `getNext`.
            private lazy val internalIter = readCurrentFile()

            override def getNext(): AnyRef = {
              try {
                if (internalIter.hasNext) {
                  internalIter.next()
                } else {
                  finished = true
                  null
                }
              } catch {
                case e: FileNotFoundException if ignoreMissingFiles =>
                  logWarning(s"Skipped missing file: $currentFile", e)
                  finished = true
                  null
                // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
                case e: FileNotFoundException if !ignoreMissingFiles => throw e
                case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                  logWarning(
                    s"Skipped the rest of the content in the corrupted file: $currentFile", e)
                  finished = true
                  null
              }
            }

            override def close(): Unit = {}
          }
        } else {
          readFile(file)
        }
      }

      private def getFileIterators: Seq[Iterator[Object]] = {
        split.asInstanceOf[FilePartition].files.map(getIterator)
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          logInfo(s"Reading File $currentFile")
          // Sets InputFileBlockHolder for the file block's information
          InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)
          currentIterator = getIterator(currentFile)

          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              val message = "Parquet column cannot be converted in " +
                s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
                s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
              throw new QueryExecutionException(message, e)
            case e: ParquetDecodingException =>
              if (e.getCause.isInstanceOf[SparkUpgradeException]) {
                throw e.getCause
              } else if (e.getMessage.contains("Can not read value at")) {
                val message = "Encounter error while reading parquet files. " +
                  "One possible cause: Parquet column cannot be converted in the " +
                  "corresponding files. Details: "
                throw new QueryExecutionException(message, e)
              }
              throw e
          }
        } else {
          currentFile = null
          InputFileBlockHolder.unset()
          false
        }
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    split.asInstanceOf[FilePartition].preferredLocations()
  }
}
