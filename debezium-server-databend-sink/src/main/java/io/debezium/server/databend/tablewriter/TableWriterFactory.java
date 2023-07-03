package io.debezium.server.databend.tablewriter;

import java.sql.Connection;
import java.util.Optional;
import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jdbi.v3.core.Jdbi;

@Dependent
public class TableWriterFactory {
    @ConfigProperty(name = "debezium.sink.jdbc.upsert", defaultValue = "true")
    boolean upsert;
    @ConfigProperty(name = "debezium.sink.jdbc.upsert-keep-deletes", defaultValue = "true")
    boolean upsertKeepDeletes;

    @ConfigProperty(name = "debezium.sink.jdbc.identifier-quote-char", defaultValue = "")
    Optional<String> identifierQuoteCharacter;

    public BaseTableWriter get(final Connection connection) {
        if (upsert) {
//            return new UpsertTableWriter(jdbi, identifierQuoteCharacter.orElse(""), upsertKeepDeletes);
        } else {
//            return new AppendTableWriter(jdbi, identifierQuoteCharacter.orElse(""));
        }
    }
}
