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

class RegionPartitioner(orderedRegions: Array[TiRegion])
  extends Partitioner {
  override def getPartition(key: Any): Int = {
    val bytes = key.asInstanceOf[Array[Byte]]
    val rawKey = Key.toRawKey(bytes)

    if (orderedRegions.isEmpty) {
      throw new PartitionNotFound("regions are empty")
    }

    val firstRegion = orderedRegions.head
    val lastRegion = orderedRegions.last
    if (rawKey.compareTo(getRowStartKey(firstRegion)) < 0) {
      throw new PartitionNotFound(s"key < fist region")
    } else if (rawKey.compareTo(getRowEndKey(lastRegion)) >= 0) {
      throw new PartitionNotFound("key > last region")
    } else {
      binarySearch(rawKey)
    }
  }

  private def binarySearch(key: Key): Int = {
    var l = 0
    var r = orderedRegions.length
    while (l < r) {
      val mid = l + (r - l) / 2
      val endKey = orderedRegions(mid).getEndKey
      if (Key.toRawKey(endKey).compareTo(key) <= 0) {
        l = mid + 1
      } else {
        r = mid
      }
    }
    l
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
    Key.toRawKey(region.getEndKey)
  }

  override def numPartitions: Int = {
    orderedRegions.length
  }
}

