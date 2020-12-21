package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.function.TriFunction;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.serializer.binary.BigDecimalValueHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class BinarySerializer extends BaseSerializer {

    private static final BigDecimalValueHandler BIG_DECIMAL_VALUE_HANDLER = new BigDecimalValueHandler();

    @Override
    public void open(OutputStream out) throws IOException {
        super.open(out);
        writeHeader();
    }

    @Override
    public void startRow(int numColumns) throws IOException {
        buffer.writeShort(numColumns);
    }

    @Override
    public void startField() {
    } // DO NOTHING

    @Override
    public <T> void writeField(DataType dataType,
                               TriFunction<QuerySession<T>, T, Integer, Object> function,
                               QuerySession<T> session,
                               T tuple,
                               int columnIndex) throws IOException {

        Object field = function.apply(session, tuple, columnIndex);

        if (field == null) {
            buffer.writeInt(-1);
        } else {
            writeFieldValue(dataType, field);
        }
    }

    @Override
    public void endField() {
    } // DO NOTHING

    @Override
    public void endRow() {
    } // DO NOTHING

    @Override
    public void close() throws IOException {
        buffer.writeShort(-1);
        super.close();
    }

    private void writeHeader() throws IOException {
        // 11 bytes required header
        buffer.writeBytes("PGCOPY\n\377\r\n\0");
        // 32 bit integer indicating no OID
        buffer.writeInt(0);
        // 32 bit header extension area length
        buffer.writeInt(0);
    }

    /**
     * Writes the {@code value} for a given {@code dataType} to the output
     * stream.
     *
     * @param dataType the data type
     * @param value    the value to be serialized
     * @throws IOException when an error occurs
     */
    private void writeFieldValue(DataType dataType, Object value) throws IOException {
        switch (dataType) {
            case BIGINT: {
                buffer.writeInt(8);
                buffer.writeLong((long) value);
                break;
            }

            case BOOLEAN: {
                buffer.writeInt(1);
                buffer.writeByte((boolean) value ? 1 : 0);
                break;
            }

            case BPCHAR:
            case TEXT:
            case VARCHAR: {
                String s = (String) value;
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                buffer.writeInt(bytes.length);
                buffer.write(bytes, 0, bytes.length);
                break;
            }

            case BYTEA: {
                byte[] bytes = (byte[]) value;
                buffer.writeInt(bytes.length);
                buffer.write(bytes, 0, bytes.length);
                break;
            }

            case DATE: {
                buffer.writeInt(4);
                // TODO: decide whether we will support both Date and String
                //       (and maybe int too) for the Date DataType
//                if (value instanceof Date) {
//                    buffer.writeInt(dateConverter.convert(((Date) value).toLocalDate()));
//                } else {
//                    buffer.writeInt(dateConverter.convert(LocalDate.parse(value.toString())));
//                }
                break;
            }

            case FLOAT8: {
                buffer.writeInt(8);
                buffer.writeDouble((double) value);
                break;
            }

            case INTEGER: {
                buffer.writeInt(4);
                buffer.writeInt((int) value);
                break;
            }

            case NUMERIC: {
                BIG_DECIMAL_VALUE_HANDLER.handle(buffer, value);
                break;
            }

            case REAL: {
                buffer.writeInt(4);
                buffer.writeFloat((float) value);
                break;
            }

            case SMALLINT: {
                buffer.writeInt(2);
                buffer.writeShort((Short) value);
                break;
            }

            case TIMESTAMP: {
                buffer.writeInt(8);

                // TODO: decide what to do here
//                LocalDateTime localDateTime;
//                if (value instanceof LocalDateTime) {
//                    localDateTime = (LocalDateTime) value;
//                } else {
//                    localDateTime = LocalDateTime.parse(value.toString(), GreenplumDateTime.DATETIME_FORMATTER);
//                }
//                buffer.writeLong(dateTimeConverter.convert(localDateTime));
                break;
            }

            default:
                throw new UnsupportedTypeException("Unsupported data type " + dataType);
        }
    }
}
