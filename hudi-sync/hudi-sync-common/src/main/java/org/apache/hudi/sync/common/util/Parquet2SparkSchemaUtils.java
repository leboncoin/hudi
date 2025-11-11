/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.model.FieldSchema;

import org.apache.avro.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Convert the parquet schema to spark schema' json string.
 * This code is refer to org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
 * in spark project.
 *
 * Supports full nested comment extraction from Avro schema when Schema object is provided,
 * or flat comment extraction when FieldSchema list is provided (legacy behavior).
 */
public class Parquet2SparkSchemaUtils {

  /**
   * Convert Parquet schema to Spark schema JSON with full nested comment support from Avro schema.
   * @param parquetSchema The Parquet GroupType schema
   * @param avroSchema The full Avro schema containing nested comments
   * @return JSON string representation of Spark schema with nested comments
   */
  public static String convertToSparkSchemaJson(GroupType parquetSchema, Schema avroSchema) {
    String fieldsJsonString = parquetSchema.getFields().stream().map(field -> {
      switch (field.getRepetition()) {
        case OPTIONAL:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field, avroSchema)
                  + ",\"nullable\":true,\"metadata\":{" + getNestedComment(field.getName(), avroSchema) + "}}";
        case REQUIRED:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field, avroSchema)
                  + ",\"nullable\":false,\"metadata\":{" + getNestedComment(field.getName(), avroSchema) + "}}";
        case REPEATED:
          String arrayType = arrayType(field, false);
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + arrayType
                  + ",\"nullable\":false,\"metadata\":{" + getNestedComment(field.getName(), avroSchema) + "}}";
        default:
          throw new UnsupportedOperationException("Unsupport convert " + field + " to spark sql type");
      }
    }).reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }

  /**
   * Convert Parquet schema to Spark schema JSON with flat comment support (legacy method).
   * @param parquetSchema The Parquet GroupType schema
   * @param fromStorage List of FieldSchema with flat comments
   * @return JSON string representation of Spark schema with flat comments
   */
  public static String convertToSparkSchemaJson(GroupType parquetSchema, List<FieldSchema> fromStorage) {
    String fieldsJsonString = parquetSchema.getFields().stream().map(field -> {
      switch (field.getRepetition()) {
        case OPTIONAL:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field)
                  + ",\"nullable\":true,\"metadata\":{" + getComment(field.getName(), fromStorage) + "}}";
        case REQUIRED:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field)
                  + ",\"nullable\":false,\"metadata\":{" + getComment(field.getName(), fromStorage) + "}}";
        case REPEATED:
          String arrayType = arrayType(field, false);
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + arrayType
                  + ",\"nullable\":false,\"metadata\":{" + getComment(field.getName(), fromStorage) + "}}";
        default:
          throw new UnsupportedOperationException("Unsupport convert " + field + " to spark sql type");
      }
    }).reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }

  private static String convertFieldType(Type field) {
    if (field instanceof PrimitiveType) {
      return "\"" + convertPrimitiveType((PrimitiveType) field) + "\"";
    } else {
      assert field instanceof GroupType;
      return convertGroupField((GroupType) field);
    }
  }

  private static String convertPrimitiveType(PrimitiveType field) {
    PrimitiveType.PrimitiveTypeName typeName = field.getPrimitiveTypeName();
    OriginalType originalType = field.getOriginalType();

    switch (typeName) {
      case BOOLEAN: return "boolean";
      case FLOAT: return "float";
      case DOUBLE: return "double";
      case INT32:
        if (originalType == null) {
          return "integer";
        }
        switch (originalType) {
          case INT_8: return "byte";
          case INT_16: return "short";
          case INT_32: return "integer";
          case DATE: return "date";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          default: throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      case INT64:
        if (originalType == null) {
          return "long";
        }
        switch (originalType)  {
          case INT_64: return "long";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            return "timestamp";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      case INT96: return "timestamp";

      case BINARY:
        if (originalType == null) {
          return "binary";
        }
        switch (originalType) {
          case UTF8:
          case  ENUM:
          case  JSON:
            return "string";
          case BSON: return "binary";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }

      case FIXED_LEN_BYTE_ARRAY:
        switch (originalType) {
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                  + field.getDecimalMetadata().getScale() + ")";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      default:
        throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
    }
  }

  private static String convertGroupField(GroupType field) {
    if (field.getOriginalType() == null) {
      // Handle Hudi's specific 3-level LIST/MAP patterns when OriginalType is missing
      if (field.getFieldCount() == 1) {
        Type child = field.getType(0);
        String childName = child.getName();

        // Hudi LIST pattern: wrapper -> "array" (repeated) -> elements
        if (childName.equals("array") && child.isRepetition(Type.Repetition.REPEATED)) {
          if (isElementType(child, field.getName())) {
            return arrayType(child, false);
          } else {
            // Safety check for primitive types
            if (child instanceof PrimitiveType) {
              return arrayType(child, false);
            }
            Type elementType = child.asGroupType().getType(0);
            boolean optional = elementType.isRepetition(Type.Repetition.OPTIONAL);
            return arrayType(elementType, optional);
          }
        }

        // Hudi MAP pattern: wrapper -> "key_value" (repeated group) -> key, value
        if (childName.equals("key_value") && child.isRepetition(Type.Repetition.REPEATED)
            && child instanceof GroupType) {
          GroupType keyValueType = child.asGroupType();
          if (keyValueType.getFieldCount() == 2) {
            Type keyType = keyValueType.getType(0);
            Type valueType = keyValueType.getType(1);
            boolean valueOptional = valueType.isRepetition(Type.Repetition.OPTIONAL);
            return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType)
                    + ",\"valueType\":" + convertFieldType(valueType)
                    + ",\"valueContainsNull\":" + valueOptional + "}";
          }
        }
      }

      // If it doesn't match LIST/MAP patterns, treat as regular struct
      return convertToSparkSchemaJson(field, Arrays.asList());
    }
    switch (field.getOriginalType()) {
      case LIST:
        ValidationUtils.checkArgument(field.getFieldCount() == 1, "Illegal List type: " + field);
        Type repeatedType = field.getType(0);
        if (isElementType(repeatedType, field.getName())) {
          return arrayType(repeatedType, false);
        } else {
          // Safety check: handle primitive repeated types that aren't GroupTypes
          if (repeatedType instanceof PrimitiveType) {
            return arrayType(repeatedType, false);
          }
          Type elementType = repeatedType.asGroupType().getType(0);
          boolean optional = elementType.isRepetition(OPTIONAL);
          return arrayType(elementType, optional);
        }
      case MAP:
      case MAP_KEY_VALUE:
        // Safety check: handle different MAP encoding formats
        Type firstChild = field.getType(0);
        if (firstChild instanceof PrimitiveType) {
          // Legacy 2-level MAP format: MAP -> key (primitive), value
          Type keyType = field.getType(0);
          Type valueType = field.getType(1);
          boolean valueOptional = valueType.isRepetition(OPTIONAL);
          return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType)
                  + ",\"valueType\":" + convertFieldType(valueType)
                  + ",\"valueContainsNull\":" + valueOptional + "}";
        } else {
          // Modern 3-level MAP format: MAP -> key_value (group) -> key, value
          GroupType keyValueType = firstChild.asGroupType();
          Type keyType = keyValueType.getType(0);
          Type valueType = keyValueType.getType(1);
          boolean valueOptional = valueType.isRepetition(OPTIONAL);
          return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType)
                  + ",\"valueType\":" + convertFieldType(valueType)
                  + ",\"valueContainsNull\":" + valueOptional + "}";
        }
      default:
        throw new UnsupportedOperationException("Unsupport convert " + field + " to spark sql type");
    }
  }

  private static String arrayType(Type elementType, boolean containsNull) {
    return "{\"type\":\"array\", \"elementType\":" + convertFieldType(elementType) + ",\"containsNull\":" + containsNull + "}";
  }

  private static boolean isElementType(Type repeatedType, String parentName) {
    return repeatedType.isPrimitive()
      || (repeatedType instanceof GroupType && repeatedType.asGroupType().getFieldCount() > 1)
      || repeatedType.getName().equals("array")
      || repeatedType.getName().equals(parentName + "_tuple");
  }

  /**
   * Extract comment from Avro schema for a specific field path (supports nested fields).
   * @param fieldName The field name to look for
   * @param avroSchema The Avro schema to search in
   * @return JSON comment string or empty string
   */
  private static String getNestedComment(String fieldName, Schema avroSchema) {
    if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
      return "";
    }

    // Look for direct field match
    for (Schema.Field field : avroSchema.getFields()) {
      if (field.name().equals(fieldName)) {
        String doc = field.doc();
        if (doc != null && !doc.trim().isEmpty()) {
          return "\"comment\":\"" + doc.replaceAll("\"", Matcher.quoteReplacement("\\\"")) + "\"";
        }
        break;
      }
    }
    return "";
  }

  /**
   * Enhanced convertFieldType that handles nested Avro comments.
   */
  private static String convertFieldType(Type field, Schema avroSchema) {
    if (field instanceof PrimitiveType) {
      return "\"" + convertPrimitiveType((PrimitiveType) field) + "\"";
    } else {
      assert field instanceof GroupType;
      return convertGroupField((GroupType) field, avroSchema);
    }
  }

  /**
   * Enhanced convertGroupField that preserves nested comments from Avro schema.
   */
  private static String convertGroupField(GroupType field, Schema avroSchema) {
    // Find the corresponding Avro field to extract nested structure
    Schema.Field avroField = findAvroField(field.getName(), avroSchema);
    Schema nestedAvroSchema = avroField != null ? getNonNullSchema(avroField.schema()) : null;

    String fieldsJsonString = field.getFields().stream().map(subField -> {
      switch (subField.getRepetition()) {
        case OPTIONAL:
          return "{\"name\":\"" + subField.getName() + "\",\"type\":" + convertFieldType(subField, nestedAvroSchema)
              + ",\"nullable\":true,\"metadata\":{" + getNestedComment(subField.getName(), nestedAvroSchema) + "}}";
        case REQUIRED:
          return "{\"name\":\"" + subField.getName() + "\",\"type\":" + convertFieldType(subField, nestedAvroSchema)
              + ",\"nullable\":false,\"metadata\":{" + getNestedComment(subField.getName(), nestedAvroSchema) + "}}";
        case REPEATED:
          String arrayType = arrayType(subField, false);
          return "{\"name\":\"" + subField.getName() + "\",\"type\":" + arrayType
              + ",\"nullable\":false,\"metadata\":{" + getNestedComment(subField.getName(), nestedAvroSchema) + "}}";
        default:
          return "{\"name\":\"" + subField.getName() + "\",\"type\":\"null\",\"nullable\":true,\"metadata\":{}}";
      }
    }).reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }


  /**
   * Find an Avro field by name in the schema.
   */
  private static Schema.Field findAvroField(String fieldName, Schema avroSchema) {
    if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
      return null;
    }
    return avroSchema.getFields().stream()
        .filter(f -> f.name().equals(fieldName))
        .findFirst()
        .orElse(null);
  }

  /**
   * Get the non-null schema from a union (handles Avro nullable fields).
   */
  private static Schema getNonNullSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : schema.getTypes()) {
        if (unionSchema.getType() != Schema.Type.NULL) {
          return unionSchema;
        }
      }
    }
    return schema;
  }

  private static String getComment(String fieldName, List<FieldSchema> fromStorage) {
    return fromStorage.stream()
        .filter(f -> f.getName().equals(fieldName))
        .findFirst()
        .filter(f -> f.getComment().isPresent())
        .map(f -> "\"comment\":\"" + f.getComment().get().replaceAll("\"", Matcher.quoteReplacement("\\\"")) + "\"")
        .orElse("");
  }
}
