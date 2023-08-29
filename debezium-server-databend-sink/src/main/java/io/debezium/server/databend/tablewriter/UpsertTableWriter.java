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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.jooq.meta.derby.sys.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Null;


public class UpsertTableWriter extends BaseTableWriter {
    static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
    private final AppendTableWriter appendTableWriter;
    final String sourceTsMsColumn = "__source_ts_ms";
    final String opColumn = "__op";
    final String deleteColumn = "__delete";
    final boolean upsertKeepDeletes;
    protected static final Logger LOGGER = LoggerFactory.getLogger(UpsertTableWriter.class);

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
        final String upsertSql = table.preparedUpsertStatement(this.identifierQuoteCharacter);
        int inserts = 0;
        List<DatabendChangeEvent> deleteEvents = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            connection.setAutoCommit(false);

            for (DatabendChangeEvent event : events) {
                System.out.println("sjh");
                System.out.println(event.keyAsMap());
                System.out.println(event.valueAsMap());
                if (event.valueAsMap() == null) {
                    System.out.println("lalalala");
                    deleteEvents.add(event);
                } else if (upsertKeepDeletes || !event.operation().equals("d")) {
                    // NOTE: if upsertKeepDeletes = true, delete event data will insert into target table
                    Map<String, Object> values = event.valueAsMap();
                    addParametersToStatement(statement, values, event.keyAsMap());
                    statement.addBatch();
                } else if (event.operation().equals("d")) {
                    // here use soft delete
                    // if true delete, we can use this condition event.keyAsMap().containsKey(deleteColumn)
                    deleteEvents.add(event);
                }
            }

            int[] batchResult = statement.executeBatch();
            inserts = Arrays.stream(batchResult).sum();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        // handle delete event
        try {
            deleteFromTable(table, deleteEvents);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public void deleteFromTable(final RelationalTable table, final List<DatabendChangeEvent> events) throws Exception {
        for (DatabendChangeEvent event : events) {
            Map<String, Object> values = event.keyAsMap();
            String deleteSql = table.preparedDeleteStatement(this.identifierQuoteCharacter, getPrimaryKeyValue(table.primaryKey, values));
            try (PreparedStatement statement = connection.prepareStatement(deleteSql)) {
                System.out.println(deleteSql);
                statement.execute(deleteSql);
            } catch (SQLException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private void addParametersToStatement(PreparedStatement statement, Map<String, Object> parameters, Map<String, Object> keys) throws SQLException {
        int index = 1;
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Object value = entry.getValue();
            statement.setObject(index, value);
            index++;
        }
    }

    private String getPrimaryKeyValue(String primaryKey, Map<String, Object> parameters) throws Exception {
        String primaryValue = "";
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            if (Objects.equals(primaryKey, entry.getKey())) {
                primaryValue = String.valueOf(entry.getValue());
                return primaryValue;
            }
        }
        if (primaryValue.equals("")) {
            throw new Exception("No primary key set");
        }

        return primaryValue;
    }

    private List<DatabendChangeEvent> deduplicateBatch(List<DatabendChangeEvent> events) {
        ConcurrentHashMap<JsonNode, DatabendChangeEvent> deduplicatedEvents = new ConcurrentHashMap<>();
        events.stream()
                .filter(Objects::nonNull) // filter null object
                .forEach(e -> {
                    deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
                        if (oldValue != null && newValue != null && compareByTsThenOp(oldValue.value(), newValue.value()) <= 0) {
                            return newValue;
                        } else {
                            return oldValue;
                        }
                    });
                });
        return new ArrayList<>(deduplicatedEvents.values());
    }

    private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {
        if (lhs == null || rhs == null) {
            return 0;
        }
        if (lhs.get(sourceTsMsColumn) == null || rhs.get(sourceTsMsColumn) == null) {
            return 0;
        }
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
