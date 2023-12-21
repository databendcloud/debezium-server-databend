/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend.tablewriter;

import java.sql.Connection;

public class AppendTableWriter extends BaseTableWriter {
    public AppendTableWriter(Connection connection, String identifierQuoteCharacter, boolean isSchemaEvolutionEnabled) {
        super(connection, identifierQuoteCharacter, isSchemaEvolutionEnabled);
    }
}
