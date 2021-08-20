# migration
Migration tools for TiKV, e.g. online bulk load.

# How to build from source

```
mvn clean package
```
# Quick Start
```
spark-submit tikv-client-java-3.2.0-SNAPSHOT.jar --class org.tikv.bulkload.example.BulkLoadExample migration-0.0.1-SNAPSHOT.jar <pdaddr>
```

# Configuration

The configurations in the table below can be put together with `spark-defaults.conf` or passed in the same way as other Spark configuration properties.

|    Key    | Default Value |
| ---------- | --- |
| `spark.tikv.bulkload.splitRegionBackoffMS` |  `120000` |
| `spark.tikv.bulkload.scatterRegionBackoffMs` |  `30000` |
| `spark.tikv.bulkload.regionSplitNum` |  `0` |
| `spark.tikv.bulkload.minRegionSplitNum` |  `1` |
| `spark.tikv.bulkload.regionSplitKeys` |  `960000` |
| `spark.tikv.bulkload.maxRegionSplitNum` |  `64` |
| `spark.tikv.bulkload.sampleSplitFrac` |  `1000` |
| `spark.tikv.bulkload.regionSplitUsingSize` |  `true` |
| `spark.tikv.bulkload.bytesPerRegion` | `100663296` |
| `spark.tikv.bulkload.ttl` |  `-1` |
