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

package org.tikv.bulkload

import org.apache.spark.Partitioner
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion

class TiRegionSplitPartitionerV2(orderedRegions: List[TiRegion])
  extends Partitioner {
  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    if (orderedRegions.isEmpty) {
      0
    } else {
      val firstRegion = orderedRegions.head
      if (rawKey.compareTo(getRowStartKey(firstRegion)) < 0) {
        0
      } else {
        orderedRegions.indices.foreach { i =>
          val region = orderedRegions(i)
          if (rawKey.compareTo(getRowStartKey(region)) >= 0 && rawKey.compareTo(getRowEndKey(region)) < 0) {
            return i + 1
          }
        }
        orderedRegions.size + 1
      }
    }
  }

  def getRegion(key: Key): TiRegion = {
    orderedRegions.foreach { region =>
      if (key.compareTo(getRowStartKey(region)) >= 0 && key.compareTo(getRowEndKey(region)) < 0) {
        return region
      }
    }
    null
  }

  // not support in TiRegion, add manually
  private def getRowStartKey(region: TiRegion): Key = {
    if (region.getStartKey.isEmpty) return Key.MIN
    Key.toRawKey(region.getStartKey)
  }

  private def getRowEndKey(region: TiRegion): Key = {
    Key.toRawKey(region.getEndKey())
  }

  override def numPartitions: Int = {
    orderedRegions.size + 2
  }
}

