/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.server.databend.DatabendChangeEvent;
import io.debezium.server.databend.DatabendUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.*;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static io.debezium.server.databend.DatabendUtil.addParametersToStatement;

public abstract class BaseTableWriter {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTableWriter.class);
    final Connection connection;
    final String identifierQuoteCharacter;

    public BaseTableWriter(final Connection connection, String identifierQuoteCharacter) {
        this.connection = connection;
        this.identifierQuoteCharacter = identifierQuoteCharacter;
    }

    public void addToTable(final RelationalTable table, final List<DatabendChangeEvent> events) {
        final String sql = table.prepareInsertStatement(this.identifierQuoteCharacter);
        int inserts = 0;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            for (DatabendChangeEvent event : events) {
                addParametersToStatement(statement, event);
                statement.addBatch();

                int[] batchResult = statement.executeBatch();
                inserts = Arrays.stream(batchResult).sum();
                System.out.printf("insert rows %d%n", inserts);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}

