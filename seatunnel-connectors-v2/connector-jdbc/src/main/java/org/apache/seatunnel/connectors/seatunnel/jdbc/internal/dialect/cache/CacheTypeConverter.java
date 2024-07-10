package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.cache;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
@AutoService(TypeConverter.class)
public class CacheTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    public static final String Cache_NULL = "NULL";

    // -------------------------number----------------------------
    public static final String Cache_NUMERIC = "NUMERIC";
    public static final String Cache_MONEY = "MONEY";
    public static final String Cache_SMALLMONEY = "SMALLMONEY";
    public static final String Cache_NUMBER = "NUMBER";
    public static final String Cache_DEC = "DEC";
    public static final String Cache_DECIMAL = "DECIMAL";
    public static final String Cache_INTEGER = "INTEGER";
    public static final String Cache_INT = "INT";
    public static final String Cache_ROWVERSION = "ROWVERSION";
    public static final String Cache_BIGINT = "BIGINT";
    public static final String Cache_SERIAL = "SERIAL";

    public static final String Cache_TINYINT = "TINYINT";
    public static final String Cache_SMALLINT = "SMALLINT";
    public static final String Cache_MEDIUMINT = "MEDIUMINT";
    public static final String Cache_FLOAT = "FLOAT";
    public static final String Cache_DOUBLE = "DOUBLE";
    public static final String Cache_REAL = "REAL";
    public static final String Cache_DOUBLE_PRECISION = "DOUBLE PRECISION";

    // ----------------------------string-------------------------
    public static final String Cache_CHAR = "CHAR";
    public static final String Cache_CHAR_VARYING = "CHAR VARYING";
    public static final String Cache_CHARACTER_VARYING = "CHARACTER VARYING";
    public static final String Cache_NATIONAL_CHAR = "NATIONAL CHAR";
    public static final String Cache_NATIONAL_CHAR_VARYING = "NATIONAL CHAR VARYING";
    public static final String Cache_NATIONAL_CHARACTER = "NATIONAL CHARACTER";
    public static final String Cache_NATIONAL_CHARACTER_VARYING = "NATIONAL CHARACTER VARYING";
    public static final String Cache_NATIONAL_VARCHAR = "NATIONAL VARCHAR";
    public static final String Cache_NCHAR = "NCHAR";
    public static final String Cache_NVARCHAR = "NVARCHAR";
    public static final String Cache_SYSNAME = "SYSNAME";
    public static final String Cache_VARCHAR2 = "VARCHAR2";
    public static final String Cache_VARCHAR = "VARCHAR";
    public static final String Cache_UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER";
    public static final String Cache_GUID = "GUID";
    public static final String Cache_CHARACTER = "CHARACTER";
    public static final String Cache_NTEXT = "NTEXT";
    public static final String Cache_CLOB = "CLOB";
    public static final String Cache_LONG_VARCHAR = "LONG VARCHAR";
    public static final String Cache_LONG = "LONG";
    public static final String Cache_LONGTEXT = "LONGTEXT";
    public static final String Cache_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String Cache_TEXT = "TEXT";
    public static final String Cache_LONGVARCHAR = "LONGVARCHAR";

    // ------------------------------time-------------------------
    public static final String Cache_DATE = "DATE";

    public static final String Cache_TIME = "TIME";

    public static final String Cache_TIMESTAMP = "TIMESTAMP";
    public static final String Cache_POSIXTIME = "POSIXTIME";
    public static final String Cache_TIMESTAMP2 = "TIMESTAMP2";

    public static final String Cache_DATETIME = "DATETIME";
    public static final String Cache_SMALLDATETIME = "SMALLDATETIME";
    public static final String Cache_DATETIME2 = "DATETIME2";

    // ---------------------------binary---------------------------
    public static final String Cache_BINARY = "BINARY";
    public static final String Cache_VARBINARY = "VARBINARY";
    public static final String Cache_RAW = "RAW";
    public static final String Cache_LONGVARBINARY = "LONGVARBINARY";
    public static final String Cache_BINARY_VARYING = "BINARY VARYING";
    public static final String Cache_BLOB = "BLOB";
    public static final String Cache_IMAGE = "IMAGE";
    public static final String Cache_LONG_BINARY = "LONG BINARY";
    public static final String Cache_LONG_RAW = "LONG RAW";

    // ---------------------------other---------------------------
    public static final String Cache_BIT = "BIT";

    public static final int MAX_SCALE = 18;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_PRECISION = 19 + MAX_SCALE;
    public static final int DEFAULT_PRECISION = 15;
    public static final int MAX_TIME_SCALE = 9;
    public static final long GUID_LENGTH = 36;
    public static final long MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;
    public static final long MAX_BINARY_LENGTH = Integer.MAX_VALUE;
    public static final CacheTypeConverter INSTANCE = new CacheTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.CACHE;
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
        String CacheDataType = typeDefine.getDataType().toUpperCase();
        long charOrBinaryLength =
                Objects.nonNull(typeDefineLength) && typeDefineLength > 0 ? typeDefineLength : 1;
        switch (CacheDataType) {
            case Cache_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case Cache_BIT:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case Cache_NUMERIC:
            case Cache_MONEY:
            case Cache_SMALLMONEY:
            case Cache_NUMBER:
            case Cache_DEC:
            case Cache_DECIMAL:
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
            case Cache_INT:
            case Cache_INTEGER:
            case Cache_MEDIUMINT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case Cache_ROWVERSION:
            case Cache_BIGINT:
            case Cache_SERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case Cache_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case Cache_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case Cache_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case Cache_DOUBLE:
            case Cache_REAL:
            case Cache_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case Cache_CHAR:
            case Cache_CHAR_VARYING:
            case Cache_CHARACTER_VARYING:
            case Cache_NATIONAL_CHAR:
            case Cache_NATIONAL_CHAR_VARYING:
            case Cache_NATIONAL_CHARACTER:
            case Cache_NATIONAL_CHARACTER_VARYING:
            case Cache_NATIONAL_VARCHAR:
            case Cache_NCHAR:
            case Cache_SYSNAME:
            case Cache_VARCHAR2:
            case Cache_VARCHAR:
            case Cache_NVARCHAR:
            case Cache_UNIQUEIDENTIFIER:
            case Cache_GUID:
            case Cache_CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                break;
            case Cache_NTEXT:
            case Cache_CLOB:
            case Cache_LONG_VARCHAR:
            case Cache_LONG:
            case Cache_LONGTEXT:
            case Cache_MEDIUMTEXT:
            case Cache_TEXT:
            case Cache_LONGVARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(Long.valueOf(Integer.MAX_VALUE));
                break;
            case Cache_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case Cache_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case Cache_DATETIME:
            case Cache_DATETIME2:
            case Cache_SMALLDATETIME:
            case Cache_TIMESTAMP:
            case Cache_TIMESTAMP2:
            case Cache_POSIXTIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case Cache_BINARY:
            case Cache_BINARY_VARYING:
            case Cache_RAW:
            case Cache_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(charOrBinaryLength);
                break;
            case Cache_LONGVARBINARY:
            case Cache_BLOB:
            case Cache_IMAGE:
            case Cache_LONG_BINARY:
            case Cache_LONG_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(Long.valueOf(Integer.MAX_VALUE));
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.CACHE, CacheDataType, typeDefine.getName());
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
                builder.columnType(Cache_NULL);
                builder.dataType(Cache_NULL);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", Cache_VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(Cache_VARCHAR);
                } else if (column.getColumnLength() < MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", Cache_VARCHAR, column.getColumnLength()));
                    builder.dataType(Cache_VARCHAR);
                } else {
                    builder.columnType(Cache_LONG_VARCHAR);
                    builder.dataType(Cache_LONG_VARCHAR);
                }
                break;
            case BOOLEAN:
                builder.columnType(Cache_BIT);
                builder.dataType(Cache_BIT);
                break;
            case TINYINT:
                builder.columnType(Cache_TINYINT);
                builder.dataType(Cache_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(Cache_SMALLINT);
                builder.dataType(Cache_SMALLINT);
                break;
            case INT:
                builder.columnType(Cache_INTEGER);
                builder.dataType(Cache_INTEGER);
                break;
            case BIGINT:
                builder.columnType(Cache_BIGINT);
                builder.dataType(Cache_BIGINT);
                break;
            case FLOAT:
                builder.columnType(Cache_FLOAT);
                builder.dataType(Cache_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(Cache_DOUBLE);
                builder.dataType(Cache_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", Cache_DECIMAL, precision, scale));
                builder.dataType(Cache_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(Cache_LONG_BINARY);
                    builder.dataType(Cache_LONG_BINARY);
                } else if (column.getColumnLength() < MAX_BINARY_LENGTH) {
                    builder.dataType(Cache_BINARY);
                    builder.columnType(
                            String.format("%s(%s)", Cache_BINARY, column.getColumnLength()));
                } else {
                    builder.columnType(Cache_LONG_BINARY);
                    builder.dataType(Cache_LONG_BINARY);
                }
                break;
            case DATE:
                builder.columnType(Cache_DATE);
                builder.dataType(Cache_DATE);
                break;
            case TIME:
                builder.dataType(Cache_TIME);
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
                    builder.columnType(String.format("%s(%s)", Cache_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(Cache_TIME);
                }
                break;
            case TIMESTAMP:
                builder.columnType(Cache_TIMESTAMP2);
                builder.dataType(Cache_TIMESTAMP2);
                break;

            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.CACHE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
