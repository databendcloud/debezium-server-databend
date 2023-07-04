/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.eclipse.microprofile.config.Config;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class DatabendUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabendChangeConsumer.class);

    public static <T> T selectInstance(Instance<T> instances, String name) {

        Instance<T> instance = instances.select(NamedLiteral.of(name));
        if (instance.isAmbiguous()) {
            throw new DebeziumException("Multiple batch size wait class named '" + name + "' were found");
        } else if (instance.isUnsatisfied()) {
            throw new DebeziumException("No batch size wait class named '" + name + "' is available");
        }

        LOGGER.info("Using {}", instance.getClass().getName());
        return instance.get();
    }

    public static Map<String, String> getConfigSubset(Config config, String prefix) {
        final Map<String, String> ret = new HashMap<>();

        for (String propName : config.getPropertyNames()) {
            if (propName.startsWith(prefix)) {
                final String newPropName = propName.substring(prefix.length());
                ret.put(newPropName, config.getValue(propName, String.class));
            }
        }

        return ret;
    }

    private static Map<String, DataType<?>> fields(JsonNode valueSchema) {
        if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
            return fields(valueSchema, "", 0);
        }
        return new HashMap<>();
    }

    private static Map<String, DataType<?>> fields(JsonNode eventSchema, String schemaName, int columnId) {
        Map<String, DataType<?>> fields = new HashMap<>();
        String schemaType = eventSchema.get("type").textValue();
        LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
        for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
            columnId++;
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            String fieldType = jsonSchemaFieldNode.get("type").textValue();
            LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
            switch (fieldType) {
                case "array":
                    JsonNode items = jsonSchemaFieldNode.get("items");
                    if (items != null && items.has("type")) {
                        fields.put(fieldName, SQLDataType.LONGNVARCHAR);
//              String listItemType = items.get("type").textValue();
//
//              if (listItemType.equals("struct") || listItemType.equals("array") || listItemType.equals("map")) {
//                throw new RuntimeException("Complex nested array types are not supported," + " array[" + listItemType + "], field " + fieldName);
//              }
//              QualifiedType<?> item = icebergFieldType(listItemType);
//              fields.put(fieldName, QualifiedType.of(?????));
                    } else {
                        throw new RuntimeException("Unexpected Array type for field " + fieldName);
                    }
                    break;
                case "map":
                    fields.put(fieldName, SQLDataType.LONGNVARCHAR);
                    //fields.put(fieldName, QualifiedType.of(Map.class));
                    //throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
                    break;
                case "struct":
                    // create it as struct, nested type
                    fields.put(fieldName, SQLDataType.LONGNVARCHAR);
                    //Map<String, QualifiedType<?>> subSchema = fields(jsonSchemaFieldNode, fieldName, columnId);
                    //fields.put(fieldName, subSchema);
                    break;
                default: //primitive types
                    fields.put(fieldName, fieldType(fieldType));
                    break;
            }
        }

        return fields;
    }

    private static DataType<?> fieldType(String fieldType) {
        switch (fieldType) {
            case "int8":
            case "int16":
            case "int32": // int 4 bytes
                return SQLDataType.BIGINT;
            case "int64": // long 8 bytes
                return SQLDataType.BIGINT;
            case "float8":
            case "float16":
            case "float32": // float is represented in 32 bits,
                return SQLDataType.DOUBLE;
            case "float64": // double is represented in 64 bits
                return SQLDataType.DOUBLE;
            case "boolean":
                return SQLDataType.BOOLEAN;
            case "string":
                return SQLDataType.LONGNVARCHAR;
            case "uuid":
                return SQLDataType.LONGNVARCHAR;
            case "bytes":
                return SQLDataType.BINARY;
            default:
                // default to String type
                return SQLDataType.LONGNVARCHAR;
            //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
        }
    }


    public static void createTable(String schemaName, String tableName, Connection conn, DatabendChangeEvent.Schema schema,
                                   boolean upsert) {
        DSLContext create = DSL.using(conn);
        Map<String, DataType<?>> fl = DatabendUtil.fields(schema.valueSchema());
        List<Field<?>> fields = new ArrayList<>();

        fl.forEach((k, v) -> fields.add(DSL.field(DSL.name(k), v)));
        create.setSchema(schemaName);

        if (upsert && schema.keySchema() != null) {
            try (CreateTableConstraintStep sql = create.createTable(tableName)
                    .columns(fields)
                    .primaryKey(schema.keySchemaFields().keySet().toArray(new String[0]))) {
                LOGGER.warn("Creating table:\n{}", sql.getSQL());
                sql.execute();
            }
        } else {
            try (CreateTableColumnStep sql = create.createTable(tableName)
                    .columns(fields)) {
                LOGGER.warn("Creating table:\n{}", sql.getSQL());
                sql.execute();
            }
        }
    }

}