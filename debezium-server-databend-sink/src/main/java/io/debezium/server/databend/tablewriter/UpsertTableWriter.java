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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
            this.deleteUpsert(table, deduplicateBatch(events));
        } else {
            // log message
            appendTableWriter.addToTable(table, events);
        }
    }

    public void deleteUpsert(final RelationalTable table, final List<DatabendChangeEvent> events) {

        final String upsertSql = table.preparedInsertStatement(this.identifierQuoteCharacter);
        int inserts = 0;
        List<DatabendChangeEvent> deleteEvents = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            connection.setAutoCommit(false);

            for (DatabendChangeEvent event : events) {
                // NOTE: if upsertKeepDeletes = true, delete event data will insert into target table
                if (upsertKeepDeletes || !event.operation().equals("d")) {
                    Map<String, Object> values = event.valueAsMap();
                    addParametersToStatement(statement, values);
                    statement.addBatch();
                }
                if (event.operation().equals("d")) {
                    deleteEvents.add(event);
                }
            }

            int[] batchResult = statement.executeBatch();
            inserts = Arrays.stream(batchResult).sum();
            System.out.println(String.format("insert rows %d", inserts));

        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        // handle delete event

    }

    public void deleteFromTable(final RelationalTable table, final List<DatabendChangeEvent> events) {
        // TODO handle delete: sjh

    }

    private void addParametersToStatement(PreparedStatement statement, Map<String, Object> parameters) throws SQLException {
        int index = 1;
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            statement.setObject(index, entry.getValue());
            index++;
        }
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
