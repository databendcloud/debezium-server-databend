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
import org.apache.kafka.connect.data.Decimal;
import org.eclipse.microprofile.config.Config;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.meta.derby.sys.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.*;
import java.util.LinkedHashMap;

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
        Map<String, DataType<?>> fields = new LinkedHashMap<>();
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
                        fields.put(fieldName, SQLDataType.VARCHAR);
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
                    fields.put(fieldName, SQLDataType.VARCHAR);
                    //fields.put(fieldName, QualifiedType.of(Map.class));
                    //throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
                    break;
                case "struct":
                    // create it as struct, nested type
                    fields.put(fieldName, SQLDataType.VARCHAR);
                    //Map<String, QualifiedType<?>> subSchema = fields(jsonSchemaFieldNode, fieldName, columnId);
                    //fields.put(fieldName, subSchema);
                    break;
                default: //primitive types
                    fields.put(fieldName, fieldType(jsonSchemaFieldNode, fieldType));
                    break;
            }
        }

        return fields;
    }

    private static DataType<?> fieldType(JsonNode jsonSchemaFieldNode, String fieldType) {
        switch (fieldType) {
            case "int8":
            case "int16":
            case "int32": // int 4 bytes
                return SQLDataType.BIGINT;
            case "int64": // long 8 bytes
                if (jsonSchemaFieldNode.get("name") != null) {
                    String debeziumTypeName = jsonSchemaFieldNode.get("name").textValue();
                    if (debeziumTypeName.toLowerCase().contains("timestamp")) {
                        return SQLDataType.TIMESTAMP;
                    }
                }
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
                return SQLDataType.VARCHAR;
            case "bytes":
                if (jsonSchemaFieldNode.get("name") != null) {
                    String debeziumTypeName = jsonSchemaFieldNode.get("name").textValue();
                    if (debeziumTypeName.toLowerCase().contains("decimal")) {
                        JsonNode decimalParameters = jsonSchemaFieldNode.get("parameters");
                        if (decimalParameters != null && decimalParameters.has("scale")
                                && decimalParameters.has("connect.decimal.precision")) {
                            int scale = decimalParameters.get("scale").intValue();
                            int precision = decimalParameters.get("connect.decimal.precision").intValue();
                            return SQLDataType.DECIMAL(precision, scale);
                        }
                    }
                }
                return SQLDataType.BINARY;
            default:
                // default to String type
                return SQLDataType.VARCHAR;
            //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
        }
    }


    public static void createTable(String schemaName, String tableName, Connection conn, DatabendChangeEvent.Schema schema,
                                   boolean upsert) {
        DSLContext create = DSL.using(conn);
        /*schema.valueSchema():
        {"type":"struct","fields":[
        {"type":"int32","optional":false,"field":"id"},
        {"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"a"},
        {"type":"bytes","optional":true,"name":"org.apache.kafka.connect.data.Decimal","version":1,"parameters":{"scale":"2","connect.decimal.precision":"10"},"field":"b"}],
        "optional":false,"name":"from_mysql.mydb.ddd.Value"}
         */
        Map<String, DataType<?>> fl = DatabendUtil.fields(schema.valueSchema());
        List<Field<?>> fields = new ArrayList<>();
        System.out.println("valueSchema: " + schema.valueSchema());

//        fl.forEach((k, v) -> fields.add(DSL.field(DSL.name(k), v)));
        for (Map.Entry<String, DataType<?>> entry : fl.entrySet()) {
            String k = entry.getKey();
            DataType<?> dataType = entry.getValue();
            System.out.println("k: " + k + ", v: " + dataType);

            if (dataType.toString().contains("decimal")) {
                DataType<BigDecimal> decimalType = (DataType<BigDecimal>) dataType;
                int precision = decimalType.precision();
                int scale = decimalType.scale();

                // Create a Decimal field
                fields.add(DSL.field(DSL.name(k), dataType.precision(precision, scale)));
            } else {
                fields.add(DSL.field(DSL.name(k), dataType));
            }
        }
        try (CreateTableConstraintStep sql = create.createTable(tableName)
                .columns(fields)) {
            String createTableSQL = createTableSQL(schemaName, sql.getSQL(), schema);
            LOGGER.info("Creating table:\n{}", createTableSQL);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
                LOGGER.info("Created table:\n{}", createTableSQL);
            } catch (Exception e) {
                LOGGER.error("Failed to create table:\n{}", createTableSQL);
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void addParametersToStatement(PreparedStatement statement, DatabendChangeEvent event) throws SQLException {
        Map<String, Object> values = event.valueAsMap();
        //DatabendChangeEvent.Schema k = event.schema();
        Map<String, String> decimalFields = DatabendUtil.findDecimalFields(event.schema());
        System.out.println("valueSchema: " + event.schema.valueSchema());
        System.out.println("keySchema: " + event.schema.keySchema());
        System.out.println("valueAsMap" + event.valueAsMap());
        System.out.println("keyAsMap" + event.keyAsMap());
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
                statement.setObject(index++, values.get(key));
            }
        }
    }

    public static Map<String, String> findDecimalFields(DatabendChangeEvent.Schema schema) {
        Map<String, String> decimalFields = new HashMap<>();
        for (JsonNode jsonSchemaFieldNode : schema.valueSchema().get("fields")) {
            // if the field is decimal, replace it with decimal(precision,scale)
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            if (jsonSchemaFieldNode.get("type").textValue().equals("bytes")) {
                if (jsonSchemaFieldNode.get("name") != null) {
                    String debeziumTypeName = jsonSchemaFieldNode.get("name").textValue();
                    if (debeziumTypeName.toLowerCase().contains("decimal")) {
                        JsonNode decimalParameters = jsonSchemaFieldNode.get("parameters");
                        if (decimalParameters != null && decimalParameters.has("scale")
                                && decimalParameters.has("connect.decimal.precision")) {
                            String scale = decimalParameters.get("scale").textValue();
                            String precision = decimalParameters.get("connect.decimal.precision").textValue();
                            decimalFields.put(fieldName, scale);
                        }
                    }
                }
            }
        }
        return decimalFields;
    }

    public static boolean isSchemaChanged(DatabendChangeEvent.Schema schema) {
        if (schema.keySchema().isNull()) {
            return false;
        }
        String schemaNameStr = schema.keySchema().get("name").textValue();
        if (schemaNameStr.toLowerCase().contains("schemachange")) {
            return true;
        }
        return false;
    }

    private static String createTableSQL(String schemaName, String originalSQL, DatabendChangeEvent.Schema schema) {
        //"CREATE TABLE debeziumcdc_customers_append (__deleted boolean, id bigint, first_name varchar, __op varchar, __source_ts_ms bigint);";
        String[] parts = originalSQL.split("\\s", 4);
        parts[2] = schemaName + "." + parts[2];
//
        String modifiedSQL = String.join(" ", parts);
//        String modifiedSQL = originalSQL;
        System.out.println("sjh" + modifiedSQL);
        // replace `decimal` with `decimal(precision,scale)` by handling schema.valueSchema()
        for (JsonNode jsonSchemaFieldNode : schema.valueSchema().get("fields")) {
            // if the field is decimal, replace it with decimal(precision,scale)
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            if (jsonSchemaFieldNode.get("type").textValue().equals("bytes")) {
                if (jsonSchemaFieldNode.get("name") != null) {
                    String debeziumTypeName = jsonSchemaFieldNode.get("name").textValue();
                    if (debeziumTypeName.toLowerCase().contains("decimal")) {
                        JsonNode decimalParameters = jsonSchemaFieldNode.get("parameters");
                        if (decimalParameters != null && decimalParameters.has("scale")
                                && decimalParameters.has("connect.decimal.precision")) {
                            String scale = decimalParameters.get("scale").textValue();
                            String precision = decimalParameters.get("connect.decimal.precision").textValue();
                            // the modifiedSQL is `create table debezium.\"from_mysql_mydb_ddd\" (\"id\" bigint, \"a\" timestamp, \"b\" decimal)`

                            modifiedSQL = modifiedSQL.replace("\"" + fieldName + "\"" + " decimal", "\"" + fieldName + "\"" + " decimal(" + precision + "," + scale + ")");
                        }
                    }
                }
            }
        }

        return modifiedSQL;
    }

}