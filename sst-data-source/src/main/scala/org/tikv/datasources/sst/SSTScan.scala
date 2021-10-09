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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import org.tikv.br.{BackupDecoder, BackupMetaDecoder}

import java.io.File
import scala.collection.convert.ImplicitConversions.`map AsScala`

case class SSTScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    options: CaseInsensitiveStringMap)
    extends TextBasedFileScan(sparkSession, options) {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private final val BACKUP_META_FILE_NAME = "backupmeta"

  private final val TEMP_FILE_PREFIX = "SST"

  private final val TEMP_FILE_SUFFIX = ".meta"

  override val readDataSchema: StructType = SSTTable.sstSchema

  override val readPartitionSchema: StructType = StructType(new Array[StructField](0))

  override val partitionFilters: Seq[Expression] = Seq.empty

  override val dataFilters: Seq[Expression] = Seq.empty

  override def isSplitable(path: Path): Boolean = false

  override def getFileUnSplittableReason(path: Path): String = {
    "the sst datasource does not support split"
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val path = options.get(SSTDataSource.PATH)
    val backupMetaFilePath = downloadBackupMeta(path, hadoopConf)
    val backupMetaDecoder = BackupMetaDecoder.parse(backupMetaFilePath)
    val ttlEnabled = options.getBoolean(SSTDataSource.ENABLE_TTL, SSTDataSource.DEF_ENABLE_TTL)
    val backupDecoder: BackupDecoder =
      new BackupDecoder(backupMetaDecoder.getBackupMeta, ttlEnabled)
    val broadcastedBackupDecoder = sparkSession.sparkContext.broadcast(backupDecoder)
    SSTPartitionReaderFactory(broadcastedConf, broadcastedBackupDecoder)
  }

  private def downloadBackupMeta(path: String, hadoopConf: Configuration): String = {
    val fs = FileSystem.get(hadoopConf)
    val metaFilePath = findBackupMetaFile(path, fs)
    val localTmpPath = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX).getAbsolutePath
    new File(localTmpPath).deleteOnExit()

    logger.info(s"download $metaFilePath to $localTmpPath")
    fs.copyToLocalFile(metaFilePath, new Path(s"file://$localTmpPath"))

    localTmpPath
  }

  private def findBackupMetaFile(path: String, fs: FileSystem): Path = {
    val iterator = fs.listFiles(new Path(path), false)
    var metaFilePath: Path = null
    while (iterator.hasNext) {
      val file = iterator.next()
      if (file.getPath.getName.equals(BACKUP_META_FILE_NAME)) {
        metaFilePath = file.getPath
      }
    }
    metaFilePath
  }

  override def withFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileScan = ???
}
