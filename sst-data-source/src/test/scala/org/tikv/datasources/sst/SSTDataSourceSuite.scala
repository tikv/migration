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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.tikv.shade.com.google.protobuf.ByteString

import java.io.File

class SSTDataSourceSuite extends FunSuiteLike with BeforeAndAfterAll {
  private val expectCount = 1000
  private val expectKeyPrefix = "test_"
  private val expectValue = "A" * 64

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

  test("SST Data Source Test: ttl disabled") {
    doTest("src/test/resources/sst/", ttlEnabled = false)
  }

  test("SST Data Source Test: ttl enabled") {
    doTest("src/test/resources/sst_ttl/", ttlEnabled = true)
  }

  private def doTest(sstPath: String, ttlEnabled: Boolean): Unit = {
    val sstFilePath = if (new File(sstPath).exists()) {
      sstPath
    } else {
      s"sst-data-source/$sstPath"
    }

    val df = spark.read
      .format("sst")
      .option("enable-ttl", ttlEnabled)
      .load(sstFilePath)

    // check count
    assertResult(expectCount)(df.count())

    // check key value
    df.collect().foreach { row =>
      val key = row.get(0).asInstanceOf[Array[Byte]]
      val value = row.get(1).asInstanceOf[Array[Byte]]
      assert(ByteString.copyFrom(key).toStringUtf8.startsWith(expectKeyPrefix))
      assertResult(expectValue)(ByteString.copyFrom(value).toStringUtf8)
    }
  }
}
