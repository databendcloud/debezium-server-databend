/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import io.debezium.server.databend.DatabendChangeEvent;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.jdbi.v3.core.statement.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        try {
            Statement stmt = connection.prepareStatement(sql);
        }catch (Exception e) {

        }

//        int inserts = jdbi.withHandle(handle -> {
//            PreparedBatch b = handle.prepareBatch(sql);
//            events.forEach(e -> b.add(e.valueAsMap()));
//            return Arrays.stream(b.execute()).sum();
//        });
    }

}
