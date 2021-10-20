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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.tikv.br.{BackupDecoder, SSTDecoder}

import java.io.File

class SSTPartitionReader(sstFilePath: String, backupDecoder: BackupDecoder)
    extends PartitionReader[InternalRow] {
  private val sstDecoder: SSTDecoder = backupDecoder.decodeSST(sstFilePath)
  private val iterator = sstDecoder.getIterator

  private var current: Option[InternalRow] = None

  override def next(): Boolean = {
    if (iterator.hasNext) {
      val nextPair = iterator.next()
      val row = new GenericInternalRow(2)
      row.update(0, nextPair.first.toByteArray)
      row.update(1, nextPair.second.toByteArray)
      current = Some(row)
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current.orNull

  override def close(): Unit = {
    sstDecoder.close()
    new File(sstFilePath).delete()
  }
}
