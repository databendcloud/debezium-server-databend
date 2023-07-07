/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend;

import io.debezium.server.TestConfigSource;

public class ConfigSource extends TestConfigSource {

    public static final String TARGET_SCHEMA = "public";

    public ConfigSource() {
        config.put("quarkus.profile", "postgresql");
        // sink conf
        config.put("debezium.sink.type", "databend");
        config.put("debezium.sink.databend.upsert", "true");
        config.put("debezium.sink.databend.database.url","jdbc:databend://localhost:8000");
        config.put("debezium.sink.databend.database.password","databend");
        config.put("debezium.sink.databend.database.username","databend");
        config.put("debezium.sink.databend.upsert-keep-deletes", "true");
        config.put("debezium.sink.databend.database.databaseName", TARGET_SCHEMA);
        config.put("debezium.sink.databend.table-prefix", "debeziumcdc_");
        // ==== configure batch behaviour/size ====
        // Positive integer value that specifies the maximum size of each batch of events that should be processed during
        // each iteration of this connector. Defaults to 2048.
        //config.put("debezium.source.max.batch.size", "2048");
        config.put("debezium.source.decimal.handling.mode", "double");
        // enable disable schema
        config.put("debezium.format.value.schemas.enable", "true");

        // debezium unwrap message
        config.put("debezium.transforms", "unwrap");
        config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db");
        config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
        config.put("debezium.transforms.unwrap.drop.tombstones", "true");

        // DEBEZIUM SOURCE conf
        config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        config.put("debezium.source.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        config.put("debezium.source.offset.flush.interval.ms", "60000");
        config.put("debezium.source.database.server.name", "testc");
        config.put("%postgresql.debezium.source.schema.whitelist", "inventory");
        config.put("%postgresql.debezium.source.database.whitelist", "inventory");
        config.put("debezium.source.table.whitelist", "inventory.*");
        config.put("debezium.source.include.schema.changes", "false");

        config.put("quarkus.log.level", "INFO");
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
