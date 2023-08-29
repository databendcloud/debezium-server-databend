/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.databend.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.server.databend.tablewriter.BaseTableWriter;
import io.debezium.server.databend.tablewriter.RelationalTable;
import io.debezium.server.databend.tablewriter.TableNotFoundException;
import io.debezium.server.databend.tablewriter.TableWriterFactory;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jooq.meta.derby.sys.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages to databend database tables.
 *
 * @author hantmac
 */
@Named("databend")
@Dependent
public class DatabendChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
    public static final ObjectMapper mapper = new ObjectMapper();
    protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabendChangeConsumer.class);
    static Deserializer<JsonNode> valDeserializer;
    static Deserializer<JsonNode> keyDeserializer;
    protected final Clock clock = Clock.system();
    protected long consumerStart = clock.currentTimeInMillis();
    protected long numConsumedEvents = 0;
    protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
    @ConfigProperty(name = "debezium.sink.databend.destination-regexp", defaultValue = "")
    protected Optional<String> destinationRegexp;
    @ConfigProperty(name = "debezium.sink.databend.destination-regexp-replace", defaultValue = "")
    protected Optional<String> destinationRegexpReplace;
    @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
    String valueFormat;
    @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
    String keyFormat;
    public Connection connection;
    @ConfigProperty(name = "debezium.sink.databend.table-prefix", defaultValue = "")
    Optional<String> tablePrefix;
    @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
    String batchSizeWaitName;
    @ConfigProperty(name = "debezium.format.value.schemas.enable", defaultValue = "false")
    boolean eventSchemaEnabled;
    @Inject
    @Any
    Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
    InterfaceBatchSizeWait batchSizeWait;
    @Inject
    TableWriterFactory tableWriterFactory;
    BaseTableWriter tableWriter;
    @ConfigProperty(name = "debezium.sink.databend.database.databaseName", defaultValue = "debezium")
    String databaseName;
    @ConfigProperty(name = "debezium.sink.databend.database.primaryKey", defaultValue = "")
    Optional<String> primaryKey;
    @ConfigProperty(name = "debezium.sink.databend.database.url", defaultValue = "jdbc:databend://localhost:8000")
    String url;
    @ConfigProperty(name = "debezium.sink.databend.database.username", defaultValue = "databend")
    String username;
    @ConfigProperty(name = "debezium.sink.databend.database.password", defaultValue = "databend")
    String password;
    @ConfigProperty(name = "debezium.sink.databend.database.tableName", defaultValue = "")
    Optional<String> tableName;

    @ConfigProperty(name = "debezium.sink.databend.upsert", defaultValue = "true")
    boolean upsert;

    public Connection createConnection(BasicDataSource dataSource, Properties properties) throws SQLException, ClassNotFoundException {
        Class.forName("com.databend.jdbc.DatabendDriver");
        String url = dataSource.getUrl();
        return DriverManager.getConnection(url, properties);
    }

    @PostConstruct
    void connect() throws Exception {
        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new DebeziumException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
        }
        if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new DebeziumException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
        }

        batchSizeWait = DatabendUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
        batchSizeWait.initizalize();

        Properties properties = new Properties();
        Map<String, String> conf = DatabendUtil.getConfigSubset(ConfigProvider.getConfig(), "debezium.sink.databend.database.param.");
        properties.putAll(conf);
        LOGGER.trace("Databend Properties: {}", properties);
        LOGGER.trace("Databend url {}", url);
        LOGGER.trace("Databend username {}", username);
        BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        connection = createConnection(dataSource, properties);

        tableWriter = tableWriterFactory.get(connection);

        // configure and set
        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        // configure and set
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();
    }

    public RelationalTable getDatabendTable(String tableName, DatabendChangeEvent.Schema schema) throws DebeziumException {
        RelationalTable t;
        try {
            t = new RelationalTable(primaryKey.orElse(""), databaseName, tableName, connection);
        } catch (TableNotFoundException e) {
            //t = createTable;
            if (!eventSchemaEnabled) {
                throw new RuntimeException("RelationalTable '" + tableName + "' not found! Set `debezium.format.value.schemas.enable` to" +
                        " true to create tables automatically!");
            }
            // create table
            LOGGER.debug("Target table not found creating it!");
            DatabendUtil.createTable(databaseName, tableName, connection, schema, upsert);
            // get table after reading it
            t = new RelationalTable(primaryKey.orElse(""), databaseName, tableName, connection);
        }

        return t;
    }

    /**
     * @param numUploadedEvents periodically log number of events consumed
     */
    protected void logConsumerProgress(long numUploadedEvents) {
        numConsumedEvents += numUploadedEvents;
        if (logTimer.expired()) {
            LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
            numConsumedEvents = 0;
            consumerStart = clock.currentTimeInMillis();
            logTimer = Threads.timer(clock, LOG_INTERVAL);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        Instant start = Instant.now();
        LocalDateTime currentDateTime = LocalDateTime.now();
        LOGGER.info("当前时间是：{}", currentDateTime);

        //group events by destination
        Map<String, List<DatabendChangeEvent>> result =
                records.stream()
                        .map((ChangeEvent<Object, Object> e)
                                -> {
                            try {
                                return new DatabendChangeEvent(e.destination(),
                                        e.value() == null ? null : valDeserializer.deserialize(e.destination(), getBytes(e.value())),
                                        e.key() == null ? null : valDeserializer.deserialize(e.destination(), getBytes(e.key())),
                                        e.value() == null ? null : mapper.readTree(getBytes(e.value())).get("schema"),
                                        e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema")
                                );
                            } catch (IOException ex) {
                                throw new DebeziumException(ex);
                            }
                        })
                        .collect(Collectors.groupingBy(DatabendChangeEvent::destination));

        long startTime = System.nanoTime();
        // consume list of events for each destination table
        for (Map.Entry<String, List<DatabendChangeEvent>> tableEvents : result.entrySet()) {
            RelationalTable tbl = this.getDatabendTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0).schema());
            tableWriter.addToTable(tbl, tableEvents.getValue());
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        double elapsedTimeInMilliseconds = (double) elapsedTime / 1_000_000;
        LOGGER.info("当前批次 {} 行 写入 databend 时间: {} 毫秒", records.size(), elapsedTimeInMilliseconds);

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Processed event '{}'", record);
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
        this.logConsumerProgress(records.size());

        batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
    }

    public String mapDestination(String destination) {
        if (tableName.isPresent()) {
            return tablePrefix.orElse("") + tableName.orElse("");
        }
        final String getTableName = destination
                .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
                .replace(".", "_");
        return tablePrefix.orElse("") + getTableName;
    }
}
