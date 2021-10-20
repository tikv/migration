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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.shade.com.google.protobuf.ByteString

class RawKVBulkLoaderSuite extends FunSuiteLike with BeforeAndAfterAll {
  val pdAddr = "127.0.0.1:2379"
  val startKey = 1
  val middleKey = 50
  val endKey = 100
  val partitionSize = 10

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    spark = SparkSession.builder.config(sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.close()
  }

  test("insert and update with duplicate key") {
    val keyPrefix = "k_"

    val tiConf = TiConfiguration.createDefault(pdAddr)
    val rawKVBulkLoader = new RawKVBulkLoader(tiConf, spark.sparkContext.getConf)

    // insert
    val rdd = spark.sparkContext.parallelize(startKey to endKey, partitionSize).flatMap { i =>
      val item = (s"$keyPrefix$i".toArray.map(_.toByte), s"v1_$i".toArray.map(_.toByte))
      item :: item :: Nil
    }
    rawKVBulkLoader.bulkLoad(rdd)

    // assert
    val tiConf2 = TiConfiguration.createRawDefault(pdAddr)
    val tiSession = TiSession.create(tiConf2)
    val rawKVClient = tiSession.createRawClient()
    (startKey to endKey).foreach { i =>
      val key = s"$keyPrefix$i"
      val value = s"v1_$i"
      val result = rawKVClient.get(ByteString.copyFromUtf8(key))
      assert(result.isPresent)
      assertResult(value)(result.get().toStringUtf8)
    }

    // update
    val rdd2 = spark.sparkContext.parallelize(startKey to middleKey, partitionSize).map { i =>
      (s"$keyPrefix$i".toArray.map(_.toByte), s"v2_$i".toArray.map(_.toByte))
    }
    rawKVBulkLoader.bulkLoad(rdd2)
    (startKey to endKey).foreach { i =>
      val key = s"$keyPrefix$i"
      val value = if (i <= middleKey) s"v2_$i" else s"v1_$i"
      val result = rawKVClient.get(ByteString.copyFromUtf8(key))
      assert(result.isPresent)
      assertResult(value)(result.get().toStringUtf8)
    }
  }
}
