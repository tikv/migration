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
import java.util.{Comparator, UUID}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.tikv.bulkload.BulkLoadConstant._
import org.tikv.common.exception.GrpcException
import org.tikv.common.importer.{ImporterClient, SwitchTiKVModeClient}
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion
import org.tikv.common.util.{BackOffFunction, ConcreteBackOffer, FastByteComparisons, Pair}
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

  private final val logger = LoggerFactory.getLogger(getClass.getName)

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
  private val optionsMaxRegionSplitNum = sparkConf.get(MAX_REGION_SPLIT_NUM, "10240").toInt
  private val optionsSampleSplitFrac = sparkConf.get(SAMPLE_SPLIT_FRAC, "1000").toInt
  private val optionsRegionSplitUsingSize = sparkConf.get(REGION_SPLIT_USING_SIZE, "true").toBoolean
  private val optionsBytesPerRegion = sparkConf.get(BYTES_PER_REGION, "100663296").toInt

  //ttl
  private val ttl = sparkConf.get(TIME_TO_LIVE, "-1").toLong

  def bulkLoad(rdd: RDD[(Array[Byte], Array[Byte])]): Unit = {
    tiConf.setKvMode("RAW")

    tiSession = TiSession.create(tiConf)

    // calculate regionSplitPoints
    val orderedSplitPoints = getRegionSplitPoints(rdd)

    // switch to normal mode
    val switchTiKVModeClient = new SwitchTiKVModeClient(tiSession.getPDClient, tiSession.getImporterRegionStoreClientBuilder)
    switchTiKVModeClient.switchTiKVToNormalMode()

    // call region split and scatter
    tiSession.splitRegionAndScatter(
      orderedSplitPoints.asJava,
      optionsSplitRegionBackoffMS,
      optionsScatterRegionBackoffMS,
      optionsScatterWaitMS)

    // switch to import mode
    switchTiKVModeClient.keepTiKVToImportMode()

    // refetch region info
    val (minKey, maxKey) = getMinMax(rdd)

    val orderedRegions = getRegionInfo(minKey, maxKey)
    logger.info("orderedRegions size = " + orderedRegions.length)

    // repartition rdd according region
    partitioner = new RegionPartitioner(orderedRegions)
    val finalRDD = rdd.partitionBy(partitioner)
    logger.info("final partition number = " + finalRDD.getNumPartitions)

    finalRDD.foreachPartition { itor =>
      writeAndIngest(itor.map(pair => (pair._1, pair._2)), partitioner)
    }
    switchTiKVModeClient.stopKeepTiKVToImportMode()
    switchTiKVModeClient.switchTiKVToNormalMode()
    logger.info("finish to load data.")
    tiSession.close()
  }

  private def getMinMax(rdd: RDD[(Array[Byte], Array[Byte])]): (Key, Key) = {
    rdd.aggregate((Key.MAX, Key.MIN))(
      (minMax, data) => {
        val key = Key.toRawKey(data._1)
        val min = if (key.compareTo(minMax._1) < 0) {
          key
        } else {
          minMax._1
        }
        val max = if (key.compareTo(minMax._2) > 0) {
          key
        } else {
          minMax._2
        }
        (min, max)
      },
      (minMax1, minMax2) => {
        val min = if (minMax1._1.compareTo(minMax2._1) < 0) {
          minMax1._1
        } else {
          minMax2._1
        }
        val max = if (minMax1._2.compareTo(minMax2._2) > 0) {
          minMax1._2
        } else {
          minMax2._2
        }
        (min, max)
      })
  }

  private def writeAndIngest(iterator: Iterator[(Array[Byte], Array[Byte])], partitioner: RegionPartitioner): Unit = {
    val t0 = System.currentTimeMillis()
    val sortedList = new util.ArrayList[Pair[ByteString, ByteString]](iterator.toList.map(pair => Pair.create(ByteString.copyFrom(pair._1), ByteString.copyFrom(pair._2))).asJava)
    val t1 = System.currentTimeMillis()
    sortedList.sort(new Comparator[Pair[ByteString, ByteString]]() {
      override def compare(o1: Pair[ByteString, ByteString], o2: Pair[ByteString, ByteString]): Int = {
        Key.toRawKey(o1.first).compareTo(Key.toRawKey(o2.first))
      }
    })
    val t2 = System.currentTimeMillis()

    val dataSize = sortedList.size()
    if (dataSize > 0) {
      val minKey: Key = Key.toRawKey(sortedList.get(0).first)
      val maxKey: Key = Key.toRawKey(sortedList.get(sortedList.size() - 1).first)
      var region: TiRegion = partitioner.getRegion(minKey)

      logger.info(
        s"""
           |dataSize=$dataSize
           |minKey=${minKey.toByteString.toStringUtf8}
           |maxKey=${maxKey.toByteString.toStringUtf8}
           |region=$region
           |read data cost: ${(t1 - t0) / 1000}s
           |sort data cost: ${(t2 - t1) / 1000}s
           |""".stripMargin)

      if (region == null) {
        throw new Exception("region == null")
      } else {
        var uuid = genUUID()
        val backOffer = ConcreteBackOffer.newCustomBackOff(10000)
        var tiSession: TiSession = null
        while(tiSession == null) {
          try {
            tiSession = TiSession.getInstance(tiConf)
          } catch {
            case e: Throwable =>
              logger.warn("create tiSession failed!", e)
              backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoServerBusy, new Exception(e))
          }
        }

        try {
          logger.info(s"start to ingest this partition, uuid=${util.Arrays.toString(uuid)}")
          val importerClient = new ImporterClient(tiSession, ByteString.copyFrom(uuid), minKey, maxKey, region, ttl)
          importerClient.write(sortedList.iterator())
        } catch {
          case e: GrpcException if e.getMessage.contains("peer is not leader") =>
            logger.warn(s"ingest failed, uuid=${util.Arrays.toString(uuid)}", e)
            logger.info(s"retry to ingest this partition, uuid=${util.Arrays.toString(uuid)}")
            uuid = genUUID()
            tiSession.getRegionManager.invalidateRegion(region)
            region = tiSession.getRegionManager.getRegionByKey(region.getStartKey)
            val importerClient = new ImporterClient(tiSession, ByteString.copyFrom(uuid), minKey, maxKey, region, ttl)
            importerClient.write(sortedList.iterator())
        }

        val t3 = System.currentTimeMillis()
        logger.info(s"ingest cost: ${(t3 - t2) / 1000}s")
        logger.info(s"finish to ingest this partition ${util.Arrays.toString(uuid)}")
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

  private def getRegionInfo(min: Key, max: Key): Array[TiRegion] = {
    val regions = new mutable.ArrayBuffer[TiRegion]()

    tiSession.getRegionManager.invalidateAll()

    var current = min

    while (current.compareTo(max) <= 0) {
      val region = tiSession.getRegionManager.getRegionByKey(current.toByteString)
      regions.append(region)
      current = Key.toRawKey(region.getEndKey)
    }

    regions.toArray
  }

  private def getRegionSplitPoints(rdd: RDD[(Array[Byte], Array[Byte])]): List[Array[Byte]] = {
    val count = rdd.count()
    logger.info(s"total data count=$count")

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
      .sorted((x: Array[Byte], y: Array[Byte]) => {
        FastByteComparisons.compareTo(x, y)
      })
    val orderedSplitPoints = new Array[Array[Byte]](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(Array[Byte], Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }
}

