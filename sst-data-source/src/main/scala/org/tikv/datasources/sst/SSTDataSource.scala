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

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object SSTDataSource {
  // parameters
  val PATH = "path"
  val ENABLE_TTL = "enable-ttl"

  // default value
  val DEF_ENABLE_TTL = false
}

class SSTDataSource extends FileDataSourceV2 {
  override def shortName(): String = "SST"

  // do not support fallback file format
  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    SSTTable(tableName, sparkSession, optionsWithoutPaths, paths, null)
  }
}
