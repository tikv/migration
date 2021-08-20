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
