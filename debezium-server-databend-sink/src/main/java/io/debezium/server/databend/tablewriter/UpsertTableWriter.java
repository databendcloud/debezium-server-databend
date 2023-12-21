/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import io.debezium.server.databend.DatabendChangeEvent;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.server.databend.DatabendUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.debezium.server.databend.DatabendUtil.addParametersToStatement;


public class UpsertTableWriter extends BaseTableWriter {
    static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
    private final AppendTableWriter appendTableWriter;
    final String sourceTsMsColumn = "__source_ts_ms";
    final String opColumn = "__op";
    final String deleteColumn = "__delete";
    final boolean upsertKeepDeletes;
    protected static final Logger LOGGER = LoggerFactory.getLogger(UpsertTableWriter.class);

    public UpsertTableWriter(Connection connection, String identifierQuoteCharacter, boolean upsertKeepDeletes, boolean isSchemaEvolutionEnabled) {
        super(connection, identifierQuoteCharacter,isSchemaEvolutionEnabled);
        this.upsertKeepDeletes = upsertKeepDeletes;
        appendTableWriter = new AppendTableWriter(connection, identifierQuoteCharacter,isSchemaEvolutionEnabled);
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
        List<DatabendChangeEvent> schemaEvolutionEvents = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            connection.setAutoCommit(false);

            for (DatabendChangeEvent event : events) {
                if (event.valueAsMap() == null) {
                    deleteEvents.add(event);
                } else if (upsertKeepDeletes || !event.operation().equals("d")) {
                    // NOTE: if upsertKeepDeletes = true, delete event data will insert into target table
                    addParametersToStatement(statement, event);
                    statement.addBatch();
                } else if (event.operation().equals("d")) {
                    // here use soft delete
                    // if true delete, we can use this condition event.keyAsMap().containsKey(deleteColumn)
                    deleteEvents.add(event);
                } else if (DatabendUtil.isSchemaChanged(event.schema())) {
                    schemaEvolutionEvents.add(event);
                }
            }

            // Each batch needs to have the same schemas, so get the buffered records out
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

        //handle schema changed events
        try {
            schemaEvolution(schemaEvolutionEvents);
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
