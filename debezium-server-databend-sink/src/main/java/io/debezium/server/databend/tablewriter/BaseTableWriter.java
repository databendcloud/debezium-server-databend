/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.server.databend.DatabendChangeConsumer;
import io.debezium.server.databend.DatabendChangeEvent;
import io.debezium.server.databend.DatabendUtil;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.Dependent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.*;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.debezium.server.databend.DatabendUtil.addParametersToStatement;

public abstract class BaseTableWriter {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTableWriter.class);
    final Connection connection;
    final String identifierQuoteCharacter;
    final boolean isSchemaEvolutionEnabled;

    public BaseTableWriter(final Connection connection, String identifierQuoteCharacter, boolean isSchemaEvolutionEnabled) {
        this.connection = connection;
        this.identifierQuoteCharacter = identifierQuoteCharacter;
        this.isSchemaEvolutionEnabled = isSchemaEvolutionEnabled;
    }

    public void addToTable(final RelationalTable table, final List<DatabendChangeEvent> events) {
        final String sql = table.prepareInsertStatement(this.identifierQuoteCharacter);
        int inserts = 0;
        List<DatabendChangeEvent> schemaEvolutionEvents = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            for (DatabendChangeEvent event : events) {
                if (DatabendUtil.isSchemaChanged(event.schema()) && isSchemaEvolutionEnabled) {
                    schemaEvolutionEvents.add(event);
                } else {
                    addParametersToStatement(statement, event);
                    statement.addBatch();
                }
            }

            // Each batch needs to have the same schemas, so get the buffered records out
            int[] batchResult = statement.executeBatch();
            inserts = Arrays.stream(batchResult).sum();
            System.out.printf("insert rows %d%n", inserts);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        // handle schema evolution
        try {
            schemaEvolution(table, schemaEvolutionEvents);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void schemaEvolution(RelationalTable table, List<DatabendChangeEvent> events) {
        for (DatabendChangeEvent event : events) {
            Map<String, Object> values = event.valueAsMap();
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                System.out.println("Key: " + key + ", Value: " + value);
                if (entry.getKey().contains("ddl")) {
                    String ddlSql = replaceFirstWordAfterTable(entry.getValue().toString(), table.databaseName + "." + table.tableName);
                    try (PreparedStatement statement = connection.prepareStatement(ddlSql)) {
                        System.out.println(ddlSql);
                        statement.execute(ddlSql);
                    } catch (SQLException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                }
            }
        }
    }

    public static String replaceFirstWordAfterTable(String statement, String newTableName) {
        if (statement == null || newTableName == null) {
            return statement;
        }
        Pattern pattern = Pattern.compile("(?<=table )\\w+");
        Matcher matcher = pattern.matcher(statement);
        return matcher.replaceFirst(newTableName);
    }
}

