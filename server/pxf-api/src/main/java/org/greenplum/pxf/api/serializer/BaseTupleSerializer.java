package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class BaseTupleSerializer<T> implements TupleSerializer<T> {

    @Override
    public void serialize(DataOutputStream out, ColumnDescriptor[] columnDescriptors, T tuple) throws IOException {
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

                case BYTEA: {
                    writeBytes(out, tuple, columnIndex);
                    break;
                }

                case DATE: {
                    writeDate(out, tuple, columnIndex);
                    break;
                }

                case FLOAT8: {
                    writeDouble(out, tuple, columnIndex);
                    break;
                }

                case INTEGER: {
                    writeInteger(out, tuple, columnIndex);
                    break;
                }

                case NUMERIC: {
                    writeNumeric(out, tuple, columnIndex);
                    break;
                }

                case REAL: {
                    writeFloat(out, tuple, columnIndex);
                    break;
                }

                case SMALLINT: {
                    writeShort(out, tuple, columnIndex);
                    break;
                }

                case TIMESTAMP: {
                    writeTimestamp(out, tuple, columnIndex);
                    break;
                }

                default:
                    throw new UnsupportedTypeException("Unsupported data type " + dataType);
            }
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

    protected abstract void writeBytes(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeDouble(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeInteger(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeFloat(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeShort(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeDate(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeTimestamp(DataOutputStream out, T tuple, int columnIndex) throws IOException;

    protected abstract void writeNumeric(DataOutputStream out, T tuple, int columnIndex) throws IOException;
}
