/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import io.debezium.DebeziumException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationalTable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(RelationalTable.class);

    public final String tableName;
    private final String schemaName;
    public final Map<String, JDBCType> columns = new HashMap<>();
    public final Map<String, Integer> primaryKeys = new HashMap<>();

    public Connection createConnection(String database, Properties p) throws SQLException {
        String url = "jdbc:databend://localhost:8000/" + database;
        return DriverManager.getConnection(url, p);
    }

    public RelationalTable(String schemaName, String tableName, Connection conn) throws DebeziumException {
        this.schemaName = schemaName;
        this.tableName = tableName;
        Properties p = new Properties();
        p.setProperty("a", "b");
        try (Connection conn1 = createConnection("default", p)) {
            conn1.getMetaData();
        } catch (Exception e) {

        }


        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet tables = meta.getTables(null, this.schemaName, this.tableName, new String[]{"TABLE"})) {

                int numTablesFound = 0;
                while (tables != null && tables.next()) {
                    numTablesFound++;
                    String catalog = tables.getString("TABLE_CAT");
                    String schema = tables.getString("TABLE_SCHEM");
                    String table = tables.getString("TABLE_NAME");

                    // get table Columns
                    try (ResultSet tColumns = meta.getColumns(catalog, schema, tableName, null)) {
                        while (tColumns.next()) {
                            String columnName = tColumns.getString("COLUMN_NAME");
                            JDBCType datatype = JDBCType.valueOf(tColumns.getInt("DATA_TYPE"));
                            columns.put(columnName, datatype);
                        }
                    }

                    // get table PK
                    try (ResultSet pKeys = meta.getPrimaryKeys(catalog, schema, table)) {
                        while (pKeys.next()) {
                            primaryKeys.put(pKeys.getString("COLUMN_NAME"), pKeys.getInt("KEY_SEQ"));
                        }
                    }
                    LOGGER.warn("Loaded Jdbc table {}.{}.{} \nColumns:{} \nPK:{}", catalog, schema, table, columns, primaryKeys);
                }

                if (numTablesFound == 0) {
                    throw new TableNotFoundException(String.format("RelationalTable %s.%s not found", schemaName, tableName));
                }

                if (numTablesFound > 1) {
                    throw new DebeziumException(String.format("Found %s tables expecting 1", numTablesFound));
                }
            }

        } catch (SQLException e) {
            throw new DebeziumException("Failed to read table from database", e);
        }
    }

    public boolean hasPK() {
        return !primaryKeys.isEmpty();
    }

    public String tableId() {
        return String.format("%s.%s", schemaName, tableName);
    }

    public String preparedInsertStatement(String identifierQuoteCharacter) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("INSERT INTO %s%s%s.%s%s%s \n",
                identifierQuoteCharacter, schemaName, identifierQuoteCharacter, identifierQuoteCharacter, tableName, identifierQuoteCharacter));

        Set<String> fields = this.columns.keySet();

        sql.append(String.format("(%s) \n",
                fields.stream()
                        .map(f -> String.format("%s%s%s ", identifierQuoteCharacter, f, identifierQuoteCharacter))
                        .collect(Collectors.joining(", "))));

        sql.append(String.format("VALUES (%s)\n",
                fields.stream()
                        .map(f -> String.format(":%s", f))
                        .collect(Collectors.joining(", "))));

        return sql.toString().trim();
    }

    public String preparedDeleteStatement(String identifierQuoteCharacter) {

        if (!hasPK()) {
            throw new DebeziumException("Cant delete from a table without primary key!");
        }

        StringBuilder sql = new StringBuilder();
        sql.append(String.format("DELETE FROM %s%s%s.%s%s%s \nWHERE ",
                identifierQuoteCharacter, schemaName, identifierQuoteCharacter, identifierQuoteCharacter, tableName, identifierQuoteCharacter));

        Set<String> fields = this.primaryKeys.keySet();

        sql.append(String.format("%s \n",
                fields.stream()
                        .map(f -> String.format("%s%s%s = :%s ", identifierQuoteCharacter, f, identifierQuoteCharacter, f))
                        .collect(Collectors.joining("\n    AND "))));

        return sql.toString().trim();
    }

}