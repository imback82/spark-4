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

package org.apache.spark.util.dotnet

import java.io._
import java.nio.file.{Files, FileSystems}
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermission._

import scala.collection.JavaConverters._
import scala.collection.Set

import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging

/**
 * Utility methods.
 */
object Utils extends Logging {
  private val posixFilePermissions = Array(
    OWNER_READ,
    OWNER_WRITE,
    OWNER_EXECUTE,
    GROUP_READ,
    GROUP_WRITE,
    GROUP_EXECUTE,
    OTHERS_READ,
    OTHERS_WRITE,
    OTHERS_EXECUTE)

  val supportPosix: Boolean =
    FileSystems.getDefault.supportedFileAttributeViews().contains("posix")

  /**
   * Unzip a file to the given directory
   *
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {
    var zipFile: ZipFile = null
    try {
      targetDir.mkdirs()
      zipFile = new ZipFile(file)
      zipFile.getEntries.asScala.foreach { entry =>
        val targetFile = new File(targetDir, entry.getName)

        if (targetFile.exists()) {
          logWarning(
            s"Target file/directory $targetFile already exists. Skip it for now. " +
              s"Make sure this is expected.")
        } else {
          if (entry.isDirectory) {
            targetFile.mkdirs()
          } else {
            targetFile.getParentFile.mkdirs()
            val input = zipFile.getInputStream(entry)
            val output = new FileOutputStream(targetFile)
            IOUtils.copy(input, output)
            IOUtils.closeQuietly(input)
            IOUtils.closeQuietly(output)
            if (supportPosix) {
              val permissions = modeToPermissions(entry.getUnixMode)
              // When run in Unix system, permissions will be empty, thus skip
              // setting the empty permissions (which will empty the previous permissions).
              if (permissions.nonEmpty) {
                Files.setPosixFilePermissions(targetFile.toPath, permissions.asJava)
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("exception caught during decompression:" + e)
    } finally {
      ZipFile.closeQuietly(zipFile)
    }
  }

  /**
   * Normalize the Spark version by taking the first three numbers.
   * For example:
   * x.y.z => x.y.z
   * x.y.z.xxx.yyy => x.y.z
   * x.y => x.y
   *
   * @param version the Spark version to normalize
   * @return Normalized Spark version.
   */
  def normalizeSparkVersion(version: String): String = {
    version
      .split('.')
      .take(3)
      .zipWithIndex
      .map({
        case (element, index) =>
          index match {
            case 2 => element.split("\\D+").lift(0).getOrElse("")
            case _ => element
          }
      })
      .mkString(".")
  }

  private[this] def modeToPermissions(mode: Int): Set[PosixFilePermission] = {
    posixFilePermissions.zipWithIndex
      .filter { case (_, i) => (mode & (0x100 >>> i)) != 0 }
      .map(_._1)
      .toSet
  }
}
