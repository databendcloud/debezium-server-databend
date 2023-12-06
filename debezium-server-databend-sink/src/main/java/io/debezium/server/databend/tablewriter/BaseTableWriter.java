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
                Map<String, Object> values = event.valueAsMap();
                DatabendChangeEvent.Schema k = event.schema();
                Map<String, String> decimalFields = DatabendUtil.findDecimalFields(event.schema());
                int index = 1;
                for (String key : values.keySet()) {
                    if (decimalFields.containsKey(key)) {
                        Object value = values.get(key);
                        if (value instanceof byte[]) {
                            // If the value is a byte array, decode it
                            final BigDecimal decoded = new BigDecimal(new BigInteger((byte[]) value), Integer.parseInt(decimalFields.get(key)));
                            statement.setObject(index++, decoded);
                        } else if (value instanceof String) {
                            // If the value is a string, parse it to BigDecimal
                            byte[] byteArray = Base64.getDecoder().decode(value.toString());
                            final BigDecimal decoded = new BigDecimal(new BigInteger(byteArray), Integer.parseInt(decimalFields.get(key)));
                            statement.setObject(index++, decoded);
                        }
                    } else {
                        System.out.println("ssss");
                        System.out.println(values.get(key));
                        statement.setObject(index++, values.get(key));
                    }
                }
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

