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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.util.Parquet2SparkSchemaUtils;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestParquet2SparkSchemaUtils {
  private final SparkToParquetSchemaConverter spark2ParquetConverter =
          new SparkToParquetSchemaConverter(new SQLConf());
  private final SparkSqlParser parser = createSqlParser();

  private static SparkSqlParser createSqlParser() {
    try {
      return SparkSqlParser.class.getDeclaredConstructor(SQLConf.class).newInstance(new SQLConf());
    } catch (Exception ne) {
      try { // For spark 3.1, there is no constructor with SQLConf, use the default constructor
        return SparkSqlParser.class.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testConvertPrimitiveType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 string, f3 bigint,"
                    + " f4 decimal(5,2), f5 timestamp, f6 date,"
                    + " f7 short, f8 float, f9 double, f10 byte,"
                    + " f11 tinyint, f12 smallint, f13 binary, f14 boolean");

    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test type with nullable
    StructField field0 = new StructField("f0", StringType$.MODULE$, false, Metadata.empty());
    StructField field1 = new StructField("f1", StringType$.MODULE$, true, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    String sparkSchemaWithNullableJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchemaWithNullable).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }

  @Test
  public void testConvertComplexType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 map<string, int>, f2 array<decimal(10,2)>"
                    + ",f3 map<array<date>, bigint>, f4 array<array<double>>"
                    + ",f5 struct<id:int, name:string>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test complex type with nullable
    StructField field0 = new StructField("f0", new ArrayType(StringType$.MODULE$, true), false, Metadata.empty());
    StructField field1 = new StructField("f1", new MapType(StringType$.MODULE$, IntegerType$.MODULE$, true), false, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    String sparkSchemaWithNullableJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchemaWithNullable).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }

  @Test
  public void testConvertArrayOfStructs() {
    // Test array of structs - this should preserve struct type within array
    StructType sparkSchema = parser.parseTableSchema(
            "locations array<struct<latitude:double, longitude:double, address:string>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation - this is the most reliable test
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Array of structs schema should be perfectly preserved through conversion");

    // Additional validation: ensure the specific structure we expect
    String expectedElementType = "\"elementType\":{\"type\":\"struct\",\"fields\":[" +
        "{\"name\":\"latitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
        "{\"name\":\"longitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
        "{\"name\":\"address\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    assertTrue(convertedSparkSchema.json().contains(expectedElementType),
        "Array should contain struct with exact field structure: " + convertedSparkSchema.json());
  }

  @Test
  public void testConvertNestedArrays() {
    // Test array of arrays - nested arrays should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "nested_arrays array<array<string>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Nested arrays schema should be perfectly preserved through conversion");

    // Validate the exact nested array structure
    String expectedElementType = "\"elementType\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}";
    assertTrue(convertedSparkSchema.json().contains(expectedElementType),
        "Nested array should have exact structure: " + convertedSparkSchema.json());
  }

  @Test
  public void testConvertArrayOfMaps() {
    // Test array of maps - should preserve map structure within arrays
    StructType sparkSchema = parser.parseTableSchema(
            "array_of_maps array<map<string,int>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Array of maps schema should be perfectly preserved through conversion");

    // Validate the exact map structure within array
    String expectedElementType = "\"elementType\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}";
    assertTrue(convertedSparkSchema.json().contains(expectedElementType),
        "Array should contain map with exact key-value structure: " + convertedSparkSchema.json());
  }

  @Test
  public void testConvertMapWithStructValues() {
    // Test map with struct values - struct values should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "map_with_structs map<string,struct<id:int,name:string>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation - the gold standard test
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Map with struct values schema should be perfectly preserved through conversion");
  }

  @Test
  public void testConvertMapWithArrayValues() {
    // Test map with array values - array values should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "map_with_arrays map<string,array<double>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Map with array values schema should be perfectly preserved through conversion");
  }

  @Test
  public void testConvertNestedStructs() {
    // Test struct with nested struct - nested structures should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "nested_struct struct<person:struct<name:string,age:int>,address:struct<city:string,country:string>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Nested structs schema should be perfectly preserved through conversion");
  }

  @Test
  public void testConvertStructWithArrays() {
    // Test struct containing arrays - arrays within structs should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "struct_with_arrays struct<tags:array<string>,scores:array<double>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Struct with arrays schema should be perfectly preserved through conversion");
  }

  @Test
  public void testConvertStructWithMaps() {
    // Test struct containing maps - maps within structs should be preserved
    StructType sparkSchema = parser.parseTableSchema(
            "struct_with_maps struct<metadata:map<string,string>,counters:map<string,int>>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // Verify exact schema preservation
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Struct with maps schema should be perfectly preserved through conversion");
  }

  @Test
  public void testConvertComplexNestedStructure() {
    // Test the most complex nested structure combining all types
    StructType sparkSchema = parser.parseTableSchema(
            "complex_data struct<" +
            "locations:array<struct<lat:double,lng:double,metadata:map<string,string>>>," +
            "user_data:map<string,struct<profile:struct<name:string,age:int>,tags:array<string>>>," +
            "nested_arrays:array<array<struct<id:int,values:array<double>>>>" +
            ">");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType(), Collections.emptyList());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);

    // The ultimate test: verify exact schema preservation for the most complex case
    assertEquals(sparkSchema.json(), convertedSparkSchema.json(),
        "Complex nested structure schema should be perfectly preserved through conversion");
  }
}
