## Quick Start

Online bulk load enables users to ingest a large amount of data to a TiKV cluster with limited impact on the online services.

### Install tikv-client-java

```
git clone git@github.com:tikv/client-java.git
mvn --file client-java/pom.xml clean install -DskipTests
```

### Build online-bulk-load project

```
git clone git@github.com:tikv/migration.git
cd migration
mvn clean package -DskipTests -am -pl online-bulk-load
```

### Run BulkLoadExample

```
spark-submit \
--master local[*] \
--jars /path/to/tikv-client-java-3.2.0-SNAPSHOT.jar \
--class org.tikv.bulkload.example.BulkLoadExample \
online-bulk-load/target/online-bulk-load-0.0.1-SNAPSHOT.jar \
<pdaddr> <key_prefix> <data_size> <partition_nums>
```

### Call RawKVBulkLoader

Also we can write a self-contained application to read parquet files.

Suppose the schema of parquet file is `key` | `value` which type is `String`.

```scala
  def main(args: Array[String]): Unit = {
    val filePath = "YOUR_PARQUET_FILE_PATH"
    val pdaddr = "YOUR_PD_ADDRESS"
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val rdd = spark.read.parquet(filePath).rdd.map(row => {
      (row.getString(0).toArray.map(_.toByte), row.getString(1).toArray.map(_.toByte))
    })
    new RawKVBulkLoader(pdaddr).bulkLoad(rdd)
  }
```

## Spark Version

Default Spark version is 3.0.2. If you want to use other Spark version, please compile with the following command:

```
mvn clean package -DskipTests -Dspark.version.compile=3.1.1
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
| `spark.tikv.bulkload.maxRegionSplitNum` |  `10240` | The maximum number of split regions |
| `spark.tikv.bulkload.sampleSplitFrac` |  `1000` | Fraction of sample split |
| `spark.tikv.bulkload.regionSplitUsingSize` |  `true` | whether using size to split region |
| `spark.tikv.bulkload.bytesPerRegion` | `100663296` | Size in bytes per region.This requires `spark.tikv.bulkload.regionSplitUsingSize` to be set true. |
| `spark.tikv.bulkload.ttl` |  `-1` | The data's time to live |

## Develop

To format the code, please run `mvn mvn-scalafmt_2.12:format` or `mvn clean package -DskipTests`.

## Documents

- [RFC: Online Bulk Load for RawKV](https://github.com/tikv/rfcs/blob/master/text/0072-online-bulk-load-for-rawkv.md)
