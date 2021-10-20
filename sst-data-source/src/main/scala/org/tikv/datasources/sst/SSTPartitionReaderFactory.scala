/*
 * Copyright 2021 TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.datasources.sst

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{
  EmptyPartitionReader,
  FilePartitionReaderFactory
}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import org.tikv.br.BackupDecoder

import java.io.File

case class SSTPartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    broadcastedBackupDecoder: Broadcast[BackupDecoder])
    extends FilePartitionReaderFactory {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private final val TEMP_FILE_PREFIX = "SST"

  private final val TEMP_FILE_SUFFIX = ".sst"

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    if (new Path(file.filePath).getName.endsWith(".sst")) {
      val conf = broadcastedConf.value.value
      val hdfsFileSystem = FileSystem.get(conf)

      val localTmpPath = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX).getAbsolutePath
      new File(localTmpPath).deleteOnExit()

      logger.info(s"download ${file.filePath} to $localTmpPath")
      hdfsFileSystem.copyToLocalFile(new Path(file.filePath), new Path(s"file://$localTmpPath"))

      val backupDecoder: BackupDecoder = broadcastedBackupDecoder.value
      new SSTPartitionReader(localTmpPath, backupDecoder)
    } else {
      new EmptyPartitionReader()
    }
  }
}
