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
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jooq.meta.derby.sys.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;

public abstract class BaseTableWriter {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTableWriter.class);
    final Connection connection;
    final String identifierQuoteCharacter;

    public BaseTableWriter(final Connection connection, String identifierQuoteCharacter) {
        this.connection = connection;
        this.identifierQuoteCharacter = identifierQuoteCharacter;
    }

    public void addToTable(final RelationalTable table, final List<DatabendChangeEvent> events) {
        final String sql = table.preparedInsertStatement(this.identifierQuoteCharacter);
        int inserts = 0;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);

            for (DatabendChangeEvent event : events) {
                Map<String, Object> values = event.valueAsMap();
                int index = 1;
                for (String key : values.keySet()) {
                    statement.setObject(index++, values.get(key));
                }
                statement.addBatch();
            }

            int[] batchResult = statement.executeBatch();
            inserts = Arrays.stream(batchResult).sum();
            System.out.printf("insert rows %d%n", inserts);

        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}

