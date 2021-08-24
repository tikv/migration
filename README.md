# migration
Migration tools for TiKV, e.g. online bulk load.

## Build

```
cd online-bulk-load
mvn clean install
```

## Quick Start
```
spark-submit --jars tikv-client-java-3.2.0-SNAPSHOT.jar --class org.tikv.bulkload.example.BulkLoadExample migration-0.0.1-SNAPSHOT.jar <pdaddr> <key_prefix> <data_size> <partition_nums>
```
Also we can write a self-contained application to read parquet files

Suppose the schema of parquet file is `key` | `value` which type is `String`
```scala
  def main(args: Array[String]): Unit = {
    val filePath = "YOUR_PARQUET_FILE_PATH"
    val pdaddr = "YOUR_PD_ADDRESS"
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val value = spark.read.parquet(filePath).rdd.map(row => {
      (row.getString(0).toArray.map(_.toByte), row.getString(1).toArray.map(_.toByte))
    })
    new RawKVBulkLoader(pdaddr).bulkLoad(value)
  }
```

## Configuration

The configurations in the table below can be put together with `spark-defaults.conf` or passed in the same way as other Spark configuration properties.

|    Key    | Default Value | Description |
| ---------- | --- | --- |
| `spark.tikv.bulkload.splitRegionBackoffMS` |  `120000` | The backoff time of split region in milliseconds |
| `spark.tikv.bulkload.scatterRegionBackoffMs` |  `30000` | The backoff time of scatter region in milliseconds
| `spark.tikv.bulkload.regionSplitNum` |  `0` | The number of split regions |
| `spark.tikv.bulkload.minRegionSplitNum` |  `1` | The minimum number of split regions |
| `spark.tikv.bulkload.regionSplitKeys` |  `960000` | The number of keys per region |
| `spark.tikv.bulkload.maxRegionSplitNum` |  `64` | The maximum number of split regions |
| `spark.tikv.bulkload.sampleSplitFrac` |  `1000` | Fraction of sample split |
| `spark.tikv.bulkload.regionSplitUsingSize` |  `true` | whether using size to split region |
| `spark.tikv.bulkload.bytesPerRegion` | `100663296` | Size in bytes per region.This requires `spark.tikv.bulkload.regionSplitUsingSize` to be set true. |
| `spark.tikv.bulkload.ttl` |  `-1` | The data's time to live |
