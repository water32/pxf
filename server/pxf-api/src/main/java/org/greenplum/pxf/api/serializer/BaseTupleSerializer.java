package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class BaseTupleSerializer<T> implements TupleSerializer<T> {

    @Override
    public void serialize(DataOutputStream out,
                          ColumnDescriptor[] columnDescriptors,
                          T tuple) throws IOException {
        int numColumns = columnDescriptors.length;
        startRow(out, numColumns);
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            ColumnDescriptor columnDescriptor = columnDescriptors[columnIndex];
            DataType dataType = columnDescriptor.getDataType();

            startField(out);
            switch (dataType) {
                case BIGINT: {
                    writeLong(out, tuple, columnIndex);
                    break;
                }

                case BOOLEAN: {
                    writeBoolean(out, tuple, columnIndex);
                    break;
                }

                case BPCHAR:
                case TEXT:
                case VARCHAR: {
                    writeText(out, tuple, columnIndex);
                    break;
                }

//                case BYTEA: {
//                    byte[] bytes = (byte[]) value;
//                    buffer.writeInt(bytes.length);
//                    buffer.write(bytes, 0, bytes.length);
//                    break;
//                }
//
//                case DATE: {
//                    buffer.writeInt(4);
//                    // TODO: decide whether we will support both Date and String
//                    //       (and maybe int too) for the Date DataType
////                if (value instanceof Date) {
////                    buffer.writeInt(dateConverter.convert(((Date) value).toLocalDate()));
////                } else {
////                    buffer.writeInt(dateConverter.convert(LocalDate.parse(value.toString())));
////                }
//                    break;
//                }
//
//                case FLOAT8: {
//                    buffer.writeInt(8);
//                    buffer.writeDouble((double) value);
//                    break;
//                }
//
//                case INTEGER: {
//                    buffer.writeInt(4);
//                    buffer.writeInt(((Number) value).intValue());
//                    break;
//                }
//
//                case NUMERIC: {
//                    BIG_DECIMAL_VALUE_HANDLER.handle(buffer, value);
//                    break;
//                }
//
//                case REAL: {
//                    buffer.writeInt(4);
//                    buffer.writeFloat((float) value);
//                    break;
//                }
//
//                case SMALLINT: {
//                    buffer.writeInt(2);
//                    buffer.writeShort(((Number) value).shortValue());
//                    break;
//                }
//
//                case TIMESTAMP: {
//                    buffer.writeInt(8);
//
//                    // TODO: decide what to do here
////                LocalDateTime localDateTime;
////                if (value instanceof LocalDateTime) {
////                    localDateTime = (LocalDateTime) value;
////                } else {
////                    localDateTime = LocalDateTime.parse(value.toString(), GreenplumDateTime.DATETIME_FORMATTER);
////                }
////                buffer.writeLong(dateTimeConverter.convert(localDateTime));
//                    break;
//                }

                default:
                    throw new UnsupportedTypeException("Unsupported data type " + dataType);
            }


//            writeField(out, columnDescriptor.getDataType(), functions[i], tuple, i);
            endField(out);
        }
        endRow(out);
    }

    abstract void startRow(DataOutputStream out, int numColumns) throws IOException;

    abstract void startField(DataOutputStream out) throws IOException;

    abstract void endField(DataOutputStream out) throws IOException;

    abstract void endRow(DataOutputStream out) throws IOException;

    protected abstract void writeLong(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeBoolean(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeText(DataOutputStream out, T tuple, int columnIndex) throws IOException;
}
