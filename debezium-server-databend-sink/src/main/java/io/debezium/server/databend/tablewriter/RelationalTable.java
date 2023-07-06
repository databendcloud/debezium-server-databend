/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import com.databend.client.data.DatabendRawType;
import io.debezium.DebeziumException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationalTable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(RelationalTable.class);

    public final String tableName;
    private final String databaseName;
    public final Map<String, DatabendRawType> columns = new HashMap<>();
    public final Map<String, Integer> primaryKeysMap = new HashMap<>();
    public final String primaryKey;


    public RelationalTable(String primaryKey, String databaseName, String tableName, Connection conn) throws DebeziumException {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.primaryKey = primaryKey;

        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet tables = meta.getColumns(null, this.databaseName, this.tableName, null)) {

                int numTablesFound = 0;
                if (tables != null && tables.next()) {
                    numTablesFound++;
                    String catalog = tables.getString("TABLE_CAT");
                    String schema = tables.getString("TABLE_SCHEM");
                    String table = tables.getString("TABLE_NAME");

                    // get table Columns
                    try (ResultSet tColumns = meta.getColumns(catalog, schema, tableName, null)) {
                        while (tColumns.next()) {
                            String columnName = tColumns.getString("COLUMN_NAME");
                            DatabendRawType databendRawType = new DatabendRawType(columnName);
                            columns.put(columnName, databendRawType);
                        }
                    }

                    // get table PK
                    if (!primaryKey.isEmpty()) {
                        primaryKeysMap.put(primaryKey, 1);
                    }
                    LOGGER.warn("Loaded Databend table {}.{}.{} \nColumns:{} \nPK:{}", catalog, schema, table, columns, primaryKeysMap);
                }

                if (numTablesFound == 0) {
                    throw new TableNotFoundException(String.format("RelationalTable %s.%s not found", databaseName, tableName));
                }
            }

        } catch (SQLException e) {
            throw new DebeziumException("Failed to read table from database", e);
        }
    }

    public boolean hasPK() {
        return !primaryKeysMap.isEmpty();
    }

    public String tableId() {
        return String.format("%s.%s", databaseName, tableName);
    }

    public String preparedUpsertStatement(String identifierQuoteCharacter) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("REPLACE INTO %s%s%s.%s%s%s on(%s%s%s)\n", identifierQuoteCharacter, databaseName, identifierQuoteCharacter, identifierQuoteCharacter, tableName, identifierQuoteCharacter, identifierQuoteCharacter, primaryKey, identifierQuoteCharacter));
        Set<String> fields = this.columns.keySet();
//        sql.append(String.format("(%s) \n", fields.stream().map(f -> String.format("%s%s%s ", identifierQuoteCharacter, f, identifierQuoteCharacter)).collect(Collectors.joining(", "))));

        sql.append(String.format("VALUES (%s)\n", fields.stream().map(f -> "?").collect(Collectors.joining(", "))));

        return sql.toString().trim();
    }

    public String prepareInsertStatement(String identifierQuoteCharacter) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("INSERT INTO %s%s%s.%s%s%s \n", identifierQuoteCharacter, databaseName, identifierQuoteCharacter, identifierQuoteCharacter, tableName, identifierQuoteCharacter));
        Set<String> fields = this.columns.keySet();
//        sql.append(String.format("(%s) \n", fields.stream().map(f -> String.format("%s%s%s ", identifierQuoteCharacter, f, identifierQuoteCharacter)).collect(Collectors.joining(", "))));

        sql.append(String.format("VALUES (%s)\n", fields.stream().map(f -> "?").collect(Collectors.joining(", "))));

        return sql.toString().trim();
    }

    public String preparedDeleteStatement(String identifierQuoteCharacter, String deleteVal) {

        if (!hasPK()) {
            throw new DebeziumException("Cant delete from a table without primary key!");
        }

        StringBuilder sql = new StringBuilder();
        sql.append(String.format("DELETE FROM %s%s%s.%s%s%s \nWHERE ", identifierQuoteCharacter, databaseName, identifierQuoteCharacter, identifierQuoteCharacter, tableName, identifierQuoteCharacter));

        Set<String> fields = this.primaryKeysMap.keySet();

        sql.append(String.format("%s \n", fields.stream().map(f -> String.format("%s%s%s = %s ", identifierQuoteCharacter, f, identifierQuoteCharacter, deleteVal)).collect(Collectors.joining("\n    AND "))));

        return sql.toString().trim();
    }

}