/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import io.trino.jdbc.$internal.client.ClientStandardTypes;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

// reference https://trino.io/docs/current/language/types.html
@Slf4j
@AutoService(TypeConverter.class)
public class TrinoTypeConverter implements TypeConverter<BasicTypeDefine<ClientStandardTypes>> {

    // ============================data types=====================
    public static final String TRINO_NULL = "NULL";

    // -------------------------number----------------------------
    public static final String TRINO_TINYINT = "TINYINT";
    public static final String TRINO_SMALLINT = "SMALLINT";
    public static final String TRINO_INTEGER = "INTEGER";
    public static final String TRINO_INT = "INT";
    public static final String TRINO_BIGINT = "BIGINT";

    public static final String TRINO_REAL = "REAL";
    public static final String TRINO_DOUBLE = "DOUBLE";
    public static final String TRINO_DECIMAL = "DECIMAL";

    // ----------------------------string-------------------------
    public static final String TRINO_CHAR = "CHAR";
    public static final String TRINO_VARCHAR = "VARCHAR";
    //    public static final String TRINO_JSON = "json";

    // ------------------------------time-------------------------
    public static final String TRINO_DATE = "DATE";

    public static final String TRINO_TIME = "TIME";
    //    public static final String TRINO_TIME_WITH_TIME_ZONE = "time with time zone";

    public static final String TRINO_TIMESTAMP = "TIMESTAMP";
    //    public static final String TRINO_TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
    //    public static final String TRINO_INTERVAL_DAY_TO_SECOND = "interval day to second";
    //    public static final String TRINO_INTERVAL_YEAR_TO_MONTH = "interval year to month";

    // ---------------------------binary---------------------------
    public static final String TRINO_VARBINARY = "VARBINARY";

    // ---------------------------other---------------------------
    public static final String TRINO_BOOLEAN = "BOOLEAN";
    //    public static final String TRINO_UUID = "uuid";

    //    public static final String TRINO_HYPER_LOG_LOG = "HyperLogLog";
    //    public static final String TRINO_QDIGEST = "qdigest";
    //    public static final String TRINO_P4_HYPER_LOG_LOG = "P4HyperLogLog";
    //    public static final String TRINO_ROW = "row";
    //    public static final String TRINO_ARRAY = "array";
    //    public static final String TRINO_MAP = "map";
    //    public static final String TRINO_IPADDRESS = "ipaddress";
    //    public static final String TRINO_GEOMETRY = "Geometry";
    //    public static final String TRINO_SPHERICAL_GEOGRAPHY = "SphericalGeography";
    //    public static final String TRINO_BING_TILE = "BingTile";

    public static final int MAX_SCALE = 18;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_PRECISION = 19 + MAX_SCALE;
    public static final int DEFAULT_PRECISION = 15;
    public static final int MAX_TIME_SCALE = 9;
    public static final long GUID_LENGTH = 36;
    public static final long MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;
    public static final long MAX_BINARY_LENGTH = Integer.MAX_VALUE;
    public static final TrinoTypeConverter INSTANCE = new TrinoTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.TRINO;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        Long typeDefineLength = typeDefine.getLength();
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .columnLength(typeDefineLength)
                        .scale(typeDefine.getScale())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String TrinoDataType = typeDefine.getDataType().toUpperCase();
        long charOrBinaryLength =
                Objects.nonNull(typeDefineLength) && typeDefineLength > 0 ? typeDefineLength : 1;
        switch (TrinoDataType) {
            case TRINO_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case TRINO_DECIMAL:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case TRINO_INT:
            case TRINO_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case TRINO_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case TRINO_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case TRINO_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case TRINO_DOUBLE:
            case TRINO_REAL:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case TRINO_CHAR:
            case TRINO_VARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                break;
            case TRINO_DATE:
                //                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                break;
            case TRINO_TIME:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                //                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case TRINO_TIMESTAMP:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                //                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case TRINO_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(charOrBinaryLength);
                break;
            case TRINO_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.TRINO, TrinoDataType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .precision(column.getColumnLength())
                        .length(column.getColumnLength())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .scale(column.getScale())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(TRINO_NULL);
                builder.dataType(TRINO_NULL);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", TRINO_VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(TRINO_VARCHAR);
                } else if (column.getColumnLength() < MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", TRINO_VARCHAR, column.getColumnLength()));
                    builder.dataType(TRINO_VARCHAR);
                } else {

                }
                break;
            case BOOLEAN:
                break;
            case TINYINT:
                builder.columnType(TRINO_TINYINT);
                builder.dataType(TRINO_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(TRINO_SMALLINT);
                builder.dataType(TRINO_SMALLINT);
                break;
            case INT:
                builder.columnType(TRINO_INTEGER);
                builder.dataType(TRINO_INTEGER);
                break;
            case BIGINT:
                builder.columnType(TRINO_BIGINT);
                builder.dataType(TRINO_BIGINT);
                break;
            case FLOAT:
                break;
            case DOUBLE:
                builder.columnType(TRINO_DOUBLE);
                builder.dataType(TRINO_DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_SCALE,
                            precision,
                            scale);
                }
                if (precision < scale) {
                    precision = scale;
                }
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = MAX_SCALE;
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                builder.columnType(String.format("%s(%s,%s)", TRINO_DECIMAL, precision, scale));
                builder.dataType(TRINO_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case DATE:
                builder.columnType(TRINO_DATE);
                builder.dataType(TRINO_DATE);
                break;
            case TIME:
                builder.dataType(TRINO_TIME);
                if (Objects.nonNull(column.getScale()) && column.getScale() > 0) {
                    Integer timeScale = column.getScale();
                    if (timeScale > MAX_TIME_SCALE) {
                        timeScale = MAX_TIME_SCALE;
                        log.warn(
                                "The time column {} type time({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to time({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIME_SCALE,
                                timeScale);
                    }
                    builder.columnType(String.format("%s(%s)", TRINO_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(TRINO_TIME);
                }
                break;
            case TIMESTAMP:
                builder.columnType(TRINO_TIMESTAMP);
                builder.dataType(TRINO_TIMESTAMP);
                break;

            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.TRINO,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
