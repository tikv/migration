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

object BulkLoadConstant {

  val SPLIT_REGION_BACKOFF_MS = "spark.tikv.bulkload.splitRegionBackoffMS"
  val SCATTER_REGION_BACKOFF_MS = "spark.tikv.bulkload.scatterRegionBackoffMs"
  val REGION_SPLIT_NUM = "spark.tikv.bulkload.regionSplitNum"
  val MIN_REGION_SPLIT_NUM = "spark.tikv.bulkload.minRegionSplitNum"
  val REGION_SPLIT_KEYS = "spark.tikv.bulkload.regionSplitKeys"
  val MAX_REGION_SPLIT_NUM = "spark.tikv.bulkload.maxRegionSplitNum"
  val SAMPLE_SPLIT_FRAC = "spark.tikv.bulkload.sampleSplitFrac"
  val REGION_SPLIT_USING_SIZE = "spark.tikv.bulkload.regionSplitUsingSize"
  val BYTES_PER_REGION = "spark.tikv.bulkload.bytesPerRegion"
  val TIME_TO_LIVE = "spark.tikv.bulkload.ttl"

}
