package org.greenplum.pxf.plugins.jdbc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * JDBC tables resolver
 */
public class JdbcResolver extends JdbcBasePlugin implements Resolver {
    // Signifies the ERA format
    final private static String DATE_TIME_FORMATTER_SPECIFIER = " G";

    /**
     * LOCAL_DATE_FORMATTER is used to translate between String and LocalDate.
     * Examples: "1977-12-11" <-> 1977-12-11
     *           "456789-12-11" <-> +456789-12-11
     *           "0010-12-11 BC" <-> -0009-12-11
     */
    private static final DateTimeFormatter LOCAL_DATE_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
            .toFormatter();

    /**
     * LOCAL_DATE_TIME_FORMATTER is used to translate between String and LocalDateTime.
     * Examples: "1980-08-10 17:10:20" <-> 1980-08-10T17:10:20
     *           "123456-10-19 11:12:13" <-> +123456-10-19T11:12:13
     *           "1234-10-19 10:11:15.456 BC" <-> -1233-10-19T10:11:15.456
     */
    private static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2, 2, SignStyle.NORMAL).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
            .toFormatter();

    /**
     * OFFSET_DATE_TIME_FORMATTER is used to translate between String and OffsetDateTime.
     * Examples: "1980-08-10 17:10:20-07" <-> 1980-08-10T17:10:20-07
     *           "123456-10-19 11:12:13+06:30" <-> +123456-10-19T11:12:13+6:30
     *           "1234-10-19 10:11:15.456+00 BC" <-> -1233-10-19T10:11:15.456+00
     */
    private static final DateTimeFormatter OFFSET_DATE_TIME_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2, 2, SignStyle.NORMAL).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "Z")
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
            .toFormatter();

    private static final Set<DataType> DATATYPES_SUPPORTED = EnumSet.of(
            DataType.VARCHAR,
            DataType.BPCHAR,
            DataType.TEXT,
            DataType.BYTEA,
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.FLOAT8,
            DataType.REAL,
            DataType.BIGINT,
            DataType.SMALLINT,
            DataType.NUMERIC,
            DataType.TIMESTAMP,
            DataType.DATE
    );

    private static final Logger LOG = LoggerFactory.getLogger(JdbcResolver.class);

    /**
     * Creates a new instance of the JdbcResolver
     */
    public JdbcResolver() {
        super();
    }

    /**
     * Creates a new instance of the resolver with provided connection manager.
     *
     * @param connectionManager connection manager
     * @param secureLogin       the instance of the secure login
     */
    JdbcResolver(ConnectionManager connectionManager, SecureLogin secureLogin) {
        super(connectionManager, secureLogin);
    }

    /**
     * getFields() implementation
     *
     * @param row one row
     * @throws SQLException if the provided {@link OneRow} object is invalid
     */
    @Override
    public List<OneField> getFields(OneRow row) throws SQLException {
        ResultSet result = (ResultSet) row.getData();
        LinkedList<OneField> fields = new LinkedList<>();

        for (ColumnDescriptor column : columns) {
            String colName = column.columnName();
            Object value;

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();

            fields.add(oneField);

            /*
             * Non-projected columns get null values
             */
            if (!column.isProjected()) continue;

            switch (DataType.get(oneField.type)) {
                case INTEGER:
                    value = result.getInt(colName);
                    break;
                case FLOAT8:
                    value = result.getDouble(colName);
                    break;
                case REAL:
                    value = result.getFloat(colName);
                    break;
                case BIGINT:
                    value = result.getLong(colName);
                    break;
                case SMALLINT:
                    value = result.getShort(colName);
                    break;
                case BOOLEAN:
                    value = result.getBoolean(colName);
                    break;
                case BYTEA:
                    value = result.getBytes(colName);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.getString(colName);
                    break;
                case DATE:
                    // getObject() is used for date, timestamp, and timestamptz because the java.sql.* types do not support wide date ranges.
                    // JDBC API 4.2 and later should have support for these types through getObject().
                    // TODO: Add comment about why we need getObject and getDate/getTimestamp because of hive jdbc
                    if (isDateWideRange) {
                        LocalDate localDate = result.getObject(colName, LocalDate.class);
                        value = formatDateTimeValues(localDate, LOCAL_DATE_FORMATTER, GreenplumDateTime.DATE_FORMATTER);
                    } else {
                        LocalDate localDate = result.getDate(colName) == null
                                ? null
                                : result.getDate(colName).toLocalDate();
                        value = formatDateTimeValues(localDate, GreenplumDateTime.DATE_FORMATTER, LOCAL_DATE_FORMATTER);
                    }
                    break;
                case TIMESTAMP:
                    if (isDateWideRange) {
                        LocalDateTime localDateTime = result.getObject(colName, LocalDateTime.class);
                        value = formatDateTimeValues(localDateTime, LOCAL_DATE_TIME_FORMATTER, GreenplumDateTime.DATETIME_FORMATTER);
                    } else {
                        LocalDateTime localDateTime = result.getTimestamp(colName) == null
                                ? null
                                : result.getTimestamp(colName).toLocalDateTime();
                        value = formatDateTimeValues(localDateTime, GreenplumDateTime.DATETIME_FORMATTER, LOCAL_DATE_TIME_FORMATTER);
                    }
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    // OffsetDateTime is the only class that JDBC drivers will most likely to respect for returning timestamptz.
                    OffsetDateTime offsetDateTime = result.getObject(colName, OffsetDateTime.class);
                    if (isDateWideRange) {
                        value = formatDateTimeValues(offsetDateTime, OFFSET_DATE_TIME_FORMATTER, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
                    } else {
                        value = formatDateTimeValues(offsetDateTime, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER, OFFSET_DATE_TIME_FORMATTER);
                    }
                    break;
                case UUID:
                    value = result.getObject(colName, java.util.UUID.class);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Field type '%s' (column '%s') is not supported",
                                    DataType.get(oneField.type),
                                    column));
            }

            oneField.val = result.wasNull() ? null : value;
        }
        return fields;
    }

    /**
     * setFields() implementation
     *
     * @param record List of fields
     * @return OneRow with the data field containing a List of fields
     * OneFields are not reordered before being passed to Accessor; at the
     * moment, there is no way to correct the order of the fields if it is not.
     * In practice, the 'record' provided is always ordered the right way.
     * @throws UnsupportedOperationException if field of some type is not supported
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException {
        int columnIndex = 0;

        for (OneField oneField : record) {
            ColumnDescriptor column = columns.get(columnIndex++);

            DataType oneFieldType = DataType.get(oneField.type);
            DataType columnType = column.getDataType();

            if (!DATATYPES_SUPPORTED.contains(oneFieldType)) {
                throw new UnsupportedOperationException(
                        String.format("Field type '%s' (column '%s') is not supported",
                                oneFieldType, column));
            }

            if (LOG.isDebugEnabled()) {
                String valDebug;
                if (oneField.val == null) {
                    valDebug = "null";
                } else if (oneFieldType == DataType.BYTEA) {
                    valDebug = String.format("'{}'", new String((byte[]) oneField.val));
                } else {
                    valDebug = String.format("'{}'", oneField.val.toString());
                }

                LOG.debug("Column {} OneField: type {}, content {}", columnIndex, oneFieldType, valDebug);
            }

            // Convert TEXT columns into native data types
            if ((oneFieldType == DataType.TEXT) && (columnType != DataType.TEXT)) {
                oneField.type = columnType.getOID();

                if (oneField.val == null) {
                    continue;
                }

                String rawVal = (String) oneField.val;
                switch (columnType) {
                    case VARCHAR:
                    case BPCHAR:
                    case TEXT:
                    case BYTEA:
                        break;
                    case BOOLEAN:
                        oneField.val = Boolean.parseBoolean(rawVal);
                        break;
                    case INTEGER:
                        oneField.val = Integer.parseInt(rawVal);
                        break;
                    case FLOAT8:
                        oneField.val = Double.parseDouble(rawVal);
                        break;
                    case REAL:
                        oneField.val = Float.parseFloat(rawVal);
                        break;
                    case BIGINT:
                        oneField.val = Long.parseLong(rawVal);
                        break;
                    case SMALLINT:
                        oneField.val = Short.parseShort(rawVal);
                        break;
                    case NUMERIC:
                        oneField.val = new BigDecimal(rawVal);
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE:
                        if (isDateWideRange) {
                            oneField.val = parseOffsetDateTime(rawVal, OFFSET_DATE_TIME_FORMATTER, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
                        } else {
                            oneField.val = parseOffsetDateTime(rawVal, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER, OFFSET_DATE_TIME_FORMATTER);
                        }
                        break;
                    case TIMESTAMP:
                        if (isDateWideRange) {
                            oneField.val = parseLocalDateTime(rawVal, LOCAL_DATE_TIME_FORMATTER, GreenplumDateTime.DATETIME_FORMATTER);
                        } else {
                            oneField.val = parseLocalDateTime(rawVal, GreenplumDateTime.DATETIME_FORMATTER, LOCAL_DATE_TIME_FORMATTER);
                        }
                        break;
                    case DATE:
                        if (isDateWideRange) {
                            oneField.val = parseLocalDate(rawVal, LOCAL_DATE_FORMATTER, GreenplumDateTime.DATE_FORMATTER);
                        } else {
                            oneField.val = parseLocalDate(rawVal, GreenplumDateTime.DATE_FORMATTER, LOCAL_DATE_FORMATTER);
                        }
                        break;
                    case UUID:
                        oneField.val = UUID.fromString(rawVal);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Field type '%s' (column '%s') is not supported",
                                        oneFieldType, column));
                }
            }
        }

        return new OneRow(record);
    }

    /**
     * Decode OneRow object and pass all its contents to a PreparedStatement
     *
     * @param row       one row
     * @param statement PreparedStatement
     * @throws IOException  if data in a OneRow is corrupted
     * @throws SQLException if the given statement is broken
     */
    @SuppressWarnings("unchecked")
    public static void decodeOneRowToPreparedStatement(OneRow row, PreparedStatement statement) throws IOException, SQLException {
        // This is safe: OneRow comes from JdbcResolver
        List<OneField> tuple = (List<OneField>) row.getData();
        for (int i = 1; i <= tuple.size(); i++) {
            OneField field = tuple.get(i - 1);
            switch (DataType.get(field.type)) {
                case INTEGER:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setInt(i, (int) field.val);
                    }
                    break;
                case BIGINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setLong(i, (long) field.val);
                    }
                    break;
                case SMALLINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setShort(i, (short) field.val);
                    }
                    break;
                case REAL:
                    if (field.val == null) {
                        statement.setNull(i, Types.FLOAT);
                    } else {
                        statement.setFloat(i, (float) field.val);
                    }
                    break;
                case FLOAT8:
                    if (field.val == null) {
                        statement.setNull(i, Types.DOUBLE);
                    } else {
                        statement.setDouble(i, (double) field.val);
                    }
                    break;
                case BOOLEAN:
                    if (field.val == null) {
                        statement.setNull(i, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(i, (boolean) field.val);
                    }
                    break;
                case NUMERIC:
                    if (field.val == null) {
                        statement.setNull(i, Types.NUMERIC);
                    } else {
                        statement.setBigDecimal(i, (BigDecimal) field.val);
                    }
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    if (field.val == null) {
                        statement.setNull(i, Types.VARCHAR);
                    } else {
                        statement.setString(i, (String) field.val);
                    }
                    break;
                case BYTEA:
                    if (field.val == null) {
                        statement.setNull(i, Types.BINARY);
                    } else {
                        statement.setBytes(i, (byte[]) field.val);
                    }
                    break;
                case TIMESTAMP:
                case DATE:
                case TIMESTAMP_WITH_TIME_ZONE:
                case UUID:
                    statement.setObject(i, field.val);
                    break;
                default:
                    throw new IOException("The data tuple from JdbcResolver is corrupted");
            }
        }
    }

    /**
     * Formats a java.time.* datetime value using two formatters in a order depending on if DateWideRange is on.
     * If DateWideRange is off but the value successfully parses with the DateWideRange formatter, log a warning
     * to turn on the DateWideRange.
     *
     * @param datetime
     * @param formatterOne
     * @param formatterTwo
     */
    private String formatDateTimeValues(TemporalAccessor datetime, DateTimeFormatter formatterOne, DateTimeFormatter formatterTwo) throws DateTimeParseException {
        if (datetime == null) {
            return null;
        }

        String value;
        try {
            value = formatterOne.format(datetime);
            if (value.charAt(0) == '+') {
                throw new DateTimeParseException("year was too long", value, 0);
            }
        } catch (DateTimeParseException e) {
            value = formatterTwo.format(datetime);
            LOG.trace("First formatter failed, used second formatter");
        }
        return value;
    }

    private OffsetDateTime parseOffsetDateTime(String rawVal, DateTimeFormatter formatterOne, DateTimeFormatter formatterTwo) {
        OffsetDateTime value;
        try {
            value = OffsetDateTime.parse(rawVal, formatterOne);
        } catch (DateTimeParseException e) {
            value = OffsetDateTime.parse(rawVal, formatterTwo);
            LOG.trace("First formatter failed, used second formatter");
        }
        return value;
    }

    private LocalDate parseLocalDate(String rawVal, DateTimeFormatter formatterOne, DateTimeFormatter formatterTwo) {
        LocalDate value;
        try {
            value = LocalDate.parse(rawVal, formatterOne);
        } catch (DateTimeParseException e) {
            value = LocalDate.parse(rawVal, formatterTwo);
            LOG.trace("First formatter failed, used second formatter");
        }
        return value;
    }

    private LocalDateTime parseLocalDateTime(String rawVal, DateTimeFormatter formatterOne, DateTimeFormatter formatterTwo) {
        LocalDateTime value;
        try {
            value = LocalDateTime.parse(rawVal, formatterOne);
        } catch (DateTimeParseException e) {
            value = LocalDateTime.parse(rawVal, formatterTwo);
            LOG.trace("First formatter failed, used second formatter");
        }
        return value;
    }
}
