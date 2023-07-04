package io.debezium.server.databend.tablewriter;

import java.sql.Connection;
import java.util.Optional;
import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@Dependent
public class TableWriterFactory {
    @ConfigProperty(name = "debezium.sink.databend.upsert", defaultValue = "true")
    boolean upsert;
    @ConfigProperty(name = "debezium.sink.databend.upsert-keep-deletes", defaultValue = "true")
    boolean upsertKeepDeletes;

    @ConfigProperty(name = "debezium.sink.databend.identifier-quote-char", defaultValue = "")
    Optional<String> identifierQuoteCharacter;

    public BaseTableWriter get(final Connection connection) {
        if (upsert) {
            return new UpsertTableWriter(connection, identifierQuoteCharacter.orElse(""), upsertKeepDeletes);
        } else {
            return new AppendTableWriter(connection, identifierQuoteCharacter.orElse(""));
        }
    }
}
