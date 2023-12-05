# Debezium Databend Consumer

Replicates database CDC events to a databend database

## Databend Consumer

Databend debezium server consumer replicates debezium CDC events to destination databend tables. It is possible to
replicate source database one
to one or run it with `append` mode or `upsert` mode to keep all change events in databend table. When event and key
schema
enabled (`debezium.format.value.schemas.enable=true`, `debezium.format.key.schemas.enable=true`) destination databend
tables created automatically with initial job.

#### Configuration properties

| Config                                                   | Default           | Description                                                                                                                             |
|----------------------------------------------------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `debezium.sink.databend.database.url`                    | ``                | Databend database jdbc url, example: jdbc:databend://localhost:8000.                                                                    |
| `debezium.sink.databend.database.username`               | ``                | Databend database user name.                                                                                                            |
| `debezium.sink.databend.database.password`               | ``                | Databend database user password.                                                                                                        |
| `debezium.sink.databend.table-prefix`                    | ``                | Prefix added to destination table names.                                                                                                |
| `debezium.sink.databend.upsert`                          | `true`            | Running upsert mode overwriting updated rows. explained below.                                                                          |
| `debezium.sink.databend.upsert-keep-deletes`             | `true`            | With upsert mode, keeps deleted rows in target table.                                                                                   |
| `debezium.sink.databend.destination-regexp`              | ``                | Regexp to modify destination table. With this its possible to map `table_ptt1`,`table_ptt2` to `table_combined`.                        |
| `debezium.sink.databend.destination-regexp-replace`      | ``                | Regexp Replace part to modify destination table                                                                                         |
| `debezium.sink.batch.batch-size-wait`                    | `NoBatchSizeWait` | Batch size wait strategy to optimize data files and upload interval. explained below.                                                   |
| `debezium.sink.databend.database.param.{jdbc.prop.name}` |                   | Additional jdbc connection config for destination database, example: ?ssl=true here use debezium.sink.databend.database.param.ssl=true. |
| `debezium.sink.databend.database.primaryKey`             | ``                | Databend Table priamryKey for upsert mode, if upsert is true, user must set this config                                                 |

### Upsert mode

By default, Debezium Databend consumer is running with upsert mode `debezium.sink.databend.upsert=true`.
Upsert mode must set Primary Key `debezium.sink.databend.database.primaryKey` and does upsert on target table. For the tables without
Primary Key consumer falls back to append mode.

> NOTE: If in upsert mode, `debezium-server-databend` support two kind of delete mode:
> 1. `hard delete`: In hard delete mode, add `debezium.transforms.unwrap.delete.handling.mode=none`, `debezium.transforms.unwrap.drop.tombstones=false` in config file. Because Debezium generates a tombstone record for each DELETE operation. The default behavior is that ExtractNewRecordState removes tombstone records from the stream. To keep tombstone records in the stream, specify drop.tombstones=false.
> 2. `soft delete`: In soft delete mode, user must add the `__deleted` field in target databend table, and add `debezium.transforms.unwrap.delete.handling.mode=rewrite`, `debezium.transforms.unwrap.drop.tombstones=true` in config file. So now we softly delete by make `__deleted` as true.

### Append mode

Setting `debezium.sink.databend.upsert=false` will set the operation mode to append. With append mode data deduplication is
not done and all received records are appended to destination table.
> Note: If user does not set primary key config the operation mode will fall back to append even configuration is set to upsert mode

#### Keeping Deleted Records

By default `debezium.sink.databend.upsert-keep-deletes=true` keeps deletes in the Databend table, setting it to false
will do soft delete from the destination Databend table by making `__deleted` field as true.

#### debezium.transforms.unwrap.add.fields

If user has this config `debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db`, make sure your target table has `__op`, `__table`,`__source.ts_ms` and `__db` field or the target table was created by debezium server.

### Optimizing batch size 

Debezium extracts database events in real time and this could cause too frequent commits which is not optimal for batch
processing especially when near realtime data feed is sufficient. To avoid this problem following batch-size-wait
classes are used.

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile
events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size`
debezium properties


#### NoBatchSizeWait

This is default configuration by default consumer will not use any wait. All the events are consumed immediately.

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size, this strategy is more precise compared to
DynamicBatchSizeWait.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
Maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`
, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size
checked every 5 seconds

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=debezium
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=debezium
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```
### include databases and include tables

User can use `debezium.source.database.include.list=databaseName1,databaseName2` to monitor the source databases and use `debezium.source.table.include.list=databaseName.tableName1,databaseName.tableName2` to monitor the source tables.

## Debezium Event Flattening

Debezium Databend consumer requires event flattening.

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
# debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db make sure the target table has there field
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
debezium.transforms.unwrap.drop.tombstones=true
```

### Databend JDBC Configuring

All the properties starting with `debezium.sink.databend.database.param.key` are passed to Databend Jdbc connection.

```properties
debezium.sink.databend.database.param.key=value
```

### Table Name Mapping

Target Databend tables are named by following rule : `table-prefix``database.server.name`_`database`_`table`

For example:

```properties
database.server.name=databend
debezium.sink.databend.table-prefix=cdc_
```

With above config database table = `database.table` is replicated to `databend_cdc_database_table`

Or if user don't like this kind of table name mapping, just use `debezium.sink.databend.database.tableName` to point a specific name.

[Detailed documentation](https://github.com/databendcloud/databend-jdbc/blob/main/docs/Connection.md) about how to use connection parameters in a Databend jdbc connection.

### Special type convert
#### Decimal types
Debezium connectors handle decimals according to the setting of the decimal.handling.mode connector configuration property.
Specifies how the connector should handle values for DECIMAL and NUMERIC columns:
```properties
# precise (the default) represents them precisely using java.math.BigDecimal values represented in change events in a binary form.
decimal.handling.mode=precise
```

```properties
# string encodes values as formatted strings, which is easy to consume but semantic information about the real type is lost.
decimal.handling.mode=string
```

```properties
# double converts values to approximate double-precision floating-point values.
decimal.handling.mode=double
```

### Example Configuration

Read [application.properties.example](../debezium-server-databend-sink/src/main/resources/conf/application.properties.example)

### Problems and Points for improvement
- Due to the integration of Debezium Engine, full-load reading phase does not support checkpoint. After a failure, it needs to be re-read.
- Currently, only single concurrency is supported, and horizontal scaling is not supported.
- When ensuring data consistency, Debezium needs to apply locks to the read databases or tables. Global locks may cause the database to hang, while table-level locks can restrict table reads.

So perhaps in the next step, we can draw inspiration from Netflix DBLog's lock-free algorithm to improve these issues.

### Related docs

[debezium-mysql-connector](https://debezium.io/documentation/reference/2.0/connectors/mysql.html#mysql-property-table-include-list)