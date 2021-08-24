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

import java.util
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.tikv.bulkload.BulkLoadConstant._
import org.tikv.common.importer.{ImporterClient, SwitchTiKVModeClient}
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion
import org.tikv.common.util.Pair
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.collection.mutable

class RawKVBulkLoader(tiConf: TiConfiguration, sparkConf: SparkConf) extends Serializable {

  def this(pdaddr: String) {
    this(TiConfiguration.createDefault(pdaddr), new SparkConf())
  }

  def this(pdaddr: String, sparkConf: SparkConf) {
    this(TiConfiguration.createDefault(pdaddr), sparkConf)
  }

  private final val logger = LoggerFactory.getLogger(getClass.getName);

  @transient private var tiSession: TiSession = _

  private var partitioner: RegionPartitioner = _

  // region split
  val optionsSplitRegionBackoffMS: Int = sparkConf.get(SPLIT_REGION_BACKOFF_MS, "120000").toInt

  val optionsScatterRegionBackoffMS: Int = sparkConf.get(SCATTER_REGION_BACKOFF_MS, "30000").toInt
  val optionsScatterWaitMS: Int = tiConf.getScatterWaitSeconds * 1000

  // sample
  private val optionsRegionSplitNum = sparkConf.get(REGION_SPLIT_NUM, "0").toInt
  private val optionsMinRegionSplitNum = sparkConf.get(MIN_REGION_SPLIT_NUM, "1").toInt
  private val optionsRegionSplitKeys = sparkConf.get(REGION_SPLIT_KEYS, "960000").toInt
  private val optionsMaxRegionSplitNum = sparkConf.get(MAX_REGION_SPLIT_NUM, "64").toInt
  private val optionsSampleSplitFrac = sparkConf.get(SAMPLE_SPLIT_FRAC, "1000").toInt
  private val optionsRegionSplitUsingSize = sparkConf.get(REGION_SPLIT_USING_SIZE, "true").toBoolean
  private val optionsBytesPerRegion = sparkConf.get(BYTES_PER_REGION, "100663296").toInt

  //ttl
  private val ttl = sparkConf.get(TIME_TO_LIVE, "-1").toLong

  def bulkLoad(rdd: RDD[(Array[Byte], Array[Byte])]): Unit = {
    tiConf.setKvMode("RAW")

    tiSession = TiSession.create(tiConf)

    // 2 sort
    val rdd2 = rdd.map { pair =>
      (SerializableKey(pair._1), pair._2)
    }.sortByKey().persist()

    // 3 calculate regionSplitPoints
    val orderedSplitPoints = getRegionSplitPoints(rdd2)

    // 4 switch to normal mode
    val switchTiKVModeClient = new SwitchTiKVModeClient(tiSession.getPDClient, tiSession.getImporterRegionStoreClientBuilder)
    switchTiKVModeClient.switchTiKVToNormalMode()

    // 5 call region split and scatter
    tiSession.splitRegionAndScatter(
      orderedSplitPoints.map(_.bytes).asJava,
      optionsSplitRegionBackoffMS,
      optionsScatterRegionBackoffMS,
      optionsScatterWaitMS)

    // 7 switch to import mode
    switchTiKVModeClient.keepTiKVToImportMode()

    // 6 refetch region info
    val minKey = rdd2.map(p => p._1).min().getRowKey
    val maxKey = rdd2.map(p => p._1).max().getRowKey
    val orderedRegions = getRegionInfo(minKey, maxKey)
    logger.info("orderedRegions size = " + orderedRegions.size)

    //8  repartition rdd according region
    partitioner = new RegionPartitioner(orderedRegions)
    val rdd3 = rdd2.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    logger.info("rdd3.getNumPartitions = " + rdd3.getNumPartitions)

    rdd3.foreachPartition { itor =>
      writeAndIngest(itor.map(pair => (pair._1.bytes, pair._2)), partitioner)
    }
    switchTiKVModeClient.stopKeepTiKVToImportMode()
    switchTiKVModeClient.switchTiKVToNormalMode()
    logger.info("finish to load data.")
    tiSession.close()
  }

  private def writeAndIngest(iterator: Iterator[(Array[Byte], Array[Byte])], partitioner: RegionPartitioner): Unit = {
    val (itor1, tiro2) = iterator.duplicate

    var minKey: Key = Key.MAX
    var maxKey: Key = Key.MIN
    var region: TiRegion = null
    var key: Key = null
    val dataSize = itor1.map { itor =>
      key = Key.toRawKey(itor._1)

      if (region == null) {
        region = partitioner.getRegion(key)
      }

      if (key.compareTo(minKey) < 0) {
        minKey = key
      }
      if (key.compareTo(maxKey) > 0) {
        maxKey = key
      }
    }.size

    if (dataSize > 0) {
      if (region == null) {
        logger.warn("region == null, skip ingest this partition")
      } else {
        val uuid = genUUID()

        logger.info(s"start to ingest this partition ${util.Arrays.toString(uuid)}")
        val pairsIterator = tiro2.map { keyValue =>
          new Pair[ByteString, ByteString](ByteString.copyFrom(keyValue._1), ByteString.copyFrom(keyValue._2))
        }.toList.sortWith((a, b) => Key.toRawKey(a.first).compareTo(Key.toRawKey(b.first)) < 0).toIterator.asJava
        val tiSession = TiSession.create(tiConf)
        val importerClient = new ImporterClient(tiSession, ByteString.copyFrom(uuid), minKey, maxKey, region, ttl)
        importerClient.write(pairsIterator)
        logger.info(s"finish to ingest this partition ${util.Arrays.toString(uuid)}")
        tiSession.close()
      }
    }
  }

  private def genUUID(): Array[Byte] = {
    val uuid = UUID.randomUUID()

    val out = new Array[Byte](16)
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits
    for (i <- 0 until 8) {
      out(i) = ((msb >> ((7 - i) * 8)) & 0xff).toByte
    }
    for (i <- 8 until 16) {
      out(i) = ((lsb >> ((15 - i) * 8)) & 0xff).toByte
    }
    out
  }

  private def getRegionInfo(min: Key, max: Key): List[TiRegion] = {
    val regions = new mutable.ArrayBuffer[TiRegion]()

    tiSession.getRegionManager.invalidateAll()

    var current = min

    while (current.compareTo(max) <= 0) {
      val region = tiSession.getRegionManager.getRegionByKey(current.toByteString)
      regions.append(region)
      current = Key.toRawKey(region.getEndKey)
    }

    regions.toList
  }

  private def getRegionSplitPoints(rdd: RDD[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.count()

    val regionSplitPointNum = if (optionsRegionSplitNum > 0) {
      optionsRegionSplitNum
    } else {
      Math.min(
        Math.max(
          optionsMinRegionSplitNum,
          Math.ceil(count.toDouble / optionsRegionSplitKeys).toInt),
        optionsMaxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * optionsSampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = if (sampleSize < count) {
      rdd.sample(withReplacement = false, sampleSize.toDouble / count).collect()
    } else {
      rdd.collect()
    }
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (optionsRegionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= optionsBytesPerRegion / optionsRegionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / optionsBytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(optionsMinRegionSplitNum, splitPointNumUsingSize),
      optionsMaxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted((x: SerializableKey, y: SerializableKey) => {
        x.compareTo(y)
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }
}

