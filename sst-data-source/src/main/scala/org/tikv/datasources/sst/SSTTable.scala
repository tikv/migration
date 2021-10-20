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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.tikv.datasources.sst.SSTTable.sstSchema

object SSTTable {
  val sstSchema: StructType = {
    val fields = new Array[StructField](2)
    fields(0) = StructField("key", BinaryType, nullable = false)
    fields(1) = StructField("value", BinaryType, nullable = true)
    StructType(fields)
  }
}

case class SSTTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, None) {

  override lazy val schema: StructType = sstSchema

  override def formatName: String = "SST"

  override def newScanBuilder(options: CaseInsensitiveStringMap): SSTScanBuilder =
    SSTScanBuilder(sparkSession, fileIndex, sstSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = Some(sstSchema)

  // do not support write
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???
}
