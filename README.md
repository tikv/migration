# migration
Migration tools for TiKV, e.g. online bulk load.

## Build

```
mvn clean package
```
## Quick Start
```
spark-submit --jars tikv-client-java-3.2.0-SNAPSHOT.jar --class org.tikv.bulkload.example.BulkLoadExample migration-0.0.1-SNAPSHOT.jar <pdaddr>
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
