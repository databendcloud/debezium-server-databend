/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import io.debezium.server.databend.DatabendChangeEvent;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;


public class UpsertTableWriter extends BaseTableWriter {
    static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
    private final AppendTableWriter appendTableWriter;
    final String sourceTsMsColumn = "__source_ts_ms";
    final String opColumn = "__op";
    final boolean upsertKeepDeletes;

    public UpsertTableWriter(Connection connection, String identifierQuoteCharacter, boolean upsertKeepDeletes) {
        super(connection, identifierQuoteCharacter);
        this.upsertKeepDeletes = upsertKeepDeletes;
        appendTableWriter = new AppendTableWriter(connection, identifierQuoteCharacter);
    }

    @Override
    public void addToTable(final RelationalTable table, final List<DatabendChangeEvent> events) {
        if (table.hasPK()) {
            this.deleteInsert(table, deduplicateBatch(events));
        } else {
            // log message
            appendTableWriter.addToTable(table, events);
        }
    }

    public void deleteInsert(final RelationalTable table, final List<DatabendChangeEvent> events) {

//        int inserts = jdbi.withHandle(handle -> {
//            handle.begin(); // USE SINGLE TRANSACTION
//            PreparedBatch delete = handle.prepareBatch(table.preparedDeleteStatement(this.identifierQuoteCharacter));
//            PreparedBatch insert = handle.prepareBatch(table.preparedInsertStatement(this.identifierQuoteCharacter));
//
//            for (DatabendChangeEvent row : events) {
//                // if its deleted row and upsertKeepDeletes = true then  deleted record to target table
//                // else deleted records are deleted from target table
//                if (upsertKeepDeletes || !(row.operation().equals("d"))) {// anything which not an insert is upsert
//                    insert.add(row.valueAsMap());
//                }
//
//                if (!row.operation().equals("c")) { // anything which not an insert is upsert
//                    delete.add(row.keyAsMap());
//                }
//            }
//
//            int[] deleted = delete.execute();
//            int[] inserted = insert.execute();
//            handle.commit();
//            return Arrays.stream(deleted).sum() + Arrays.stream(inserted).sum();
//        });
    }

    private List<DatabendChangeEvent> deduplicateBatch(List<DatabendChangeEvent> events) {

        ConcurrentHashMap<JsonNode, DatabendChangeEvent> deduplicatedEvents = new ConcurrentHashMap<>();
        events.forEach(e ->
                // deduplicate using key(PK)
                deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
                    if (this.compareByTsThenOp(oldValue.value(), newValue.value()) <= 0) {
                        return newValue;
                    } else {
                        return oldValue;
                    }
                })
        );
        return new ArrayList<>(deduplicatedEvents.values());
    }

    private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {

        int result = Long.compare(lhs.get(sourceTsMsColumn).asLong(0), rhs.get(sourceTsMsColumn).asLong(0));

        if (result == 0) {
            // return (x < y) ? -1 : ((x == y) ? 0 : 1);
            result = cdcOperations.getOrDefault(lhs.get(opColumn).asText("c"), -1)
                    .compareTo(
                            cdcOperations.getOrDefault(rhs.get(opColumn).asText("c"), -1)
                    );
        }

        return result;
    }

}
