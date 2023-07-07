/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.databend;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.databend.client.data.DatabendRawType;
import com.databend.jdbc.DatabendColumnInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hantmac
 */
public class DatabendChangeEvent {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DatabendChangeEvent.class);
    protected final String destination;
    protected final JsonNode value;
    protected final JsonNode key;
    final Schema schema;

    public DatabendChangeEvent(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
        this.destination = destination;
        this.value = value;
        this.key = key;
        this.schema = new Schema(valueSchema, keySchema);
    }

    public JsonNode key() {
        return key;
    }

    public JsonNode value() {
        return value;
    }

    public Map<String, Object> valueAsMap() {
        return DatabendChangeConsumer.mapper.convertValue(value(), new TypeReference<>() {
        });
    }

    public Map<String, Object> keyAsMap() {
        return DatabendChangeConsumer.mapper.convertValue(key(), new TypeReference<>() {
        });
    }

    public String operation() {
        return value().get("__op").textValue();
    }

    public Schema schema() {
        return schema;
    }

    public String destination() {
        return destination;
    }


    public static class Schema {
        private final JsonNode valueSchema;
        private final JsonNode keySchema;

        public Schema(JsonNode valueSchema, JsonNode keySchema) {
            this.valueSchema = valueSchema;
            this.keySchema = keySchema;
        }

        public JsonNode valueSchema() {
            return valueSchema;
        }

        public JsonNode keySchema() {
            return keySchema;
        }

        public Map<String, DatabendColumnInfo> valueSchemaFields() {
            if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
                LOGGER.debug(valueSchema.toString());
                return fields(valueSchema, "", 0);
            }
            LOGGER.trace("Event schema not found!");
            return new HashMap<>();
        }

        public Map<String, DatabendColumnInfo> keySchemaFields() {
            if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
                LOGGER.debug(keySchema.toString());
                return fields(keySchema, "", 0);
            }
            LOGGER.trace("Key schema not found!");
            return new HashMap<>();
        }

        private Map<String, DatabendColumnInfo> fields(JsonNode eventSchema, String schemaName, int columnId) {
            Map<String, DatabendColumnInfo> fields = new HashMap<>();
            String schemaType = eventSchema.get("type").textValue();
            LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
            for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
                columnId++;
                String fieldName = jsonSchemaFieldNode.get("field").textValue();
                String fieldType = jsonSchemaFieldNode.get("type").textValue();
                LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
                DatabendRawType databendStrType = new DatabendRawType(DatabendTypes.STRING);
                switch (fieldType) {
                    case "array":
                        JsonNode items = jsonSchemaFieldNode.get("items");
                        if (items != null && items.has("type")) {
                            fields.put(fieldName, DatabendColumnInfo.newBuilder(DatabendTypes.STRING, databendStrType).build());
                        } else {
                            throw new RuntimeException("Unexpected Array type for field " + fieldName);
                        }
                        break;
                    case "map":
                    case "struct":
                        // create it as struct, nested type
                        fields.put(fieldName, DatabendColumnInfo.newBuilder("String", databendStrType).build());
                        break;
                    default: //primitive types
                        fields.put(fieldName, fieldType(fieldType));
                        break;
                }
            }

            return fields;
        }

        private DatabendColumnInfo fieldType(String fieldType) {
            switch (fieldType) {
                case "int8":
                    return DatabendColumnInfo.of(DatabendTypes.INT8, new DatabendRawType(DatabendTypes.INT8));
                case "int16":
                    return DatabendColumnInfo.of(DatabendTypes.INT16, new DatabendRawType(DatabendTypes.INT16));
                case "int32": // int 4 bytes
                    return DatabendColumnInfo.of(DatabendTypes.INT32, new DatabendRawType(DatabendTypes.INT32));
                case "int64": // long 8 bytes
                    return DatabendColumnInfo.of(DatabendTypes.INT64, new DatabendRawType(DatabendTypes.INT64));
                case "float32": // float is represented in 32 bits,
                    return DatabendColumnInfo.of(DatabendTypes.FLOAT32, new DatabendRawType(DatabendTypes.FLOAT32));
                case "float64": // double is represented in 64 bits
                    return DatabendColumnInfo.of(DatabendTypes.FLOAT64, new DatabendRawType(DatabendTypes.FLOAT64));
                case "boolean":
                    return DatabendColumnInfo.of(DatabendTypes.BOOLEAN, new DatabendRawType(DatabendTypes.BOOLEAN));
                case "string":
                    return DatabendColumnInfo.of(DatabendTypes.STRING, new DatabendRawType(DatabendTypes.STRING));
                default:
                    // default to String type
                    return DatabendColumnInfo.of(DatabendTypes.STRING, new DatabendRawType(DatabendTypes.STRING));
                //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Schema that = (Schema) o;
            return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(valueSchema, keySchema);
        }
    }

}
