package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.TupleBatch;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseTupleSerializer<T, M> implements TupleSerializer<T, M> {

    @Override
    public void open(OutputStream out, SerializerAdapter adapter) throws IOException {
        adapter.open(out);
    }

    @Override
    public void serialize(OutputStream out, ColumnDescriptor[] columnDescriptors, TupleBatch<T, M> batch, SerializerAdapter adapter) throws IOException {
        int numColumns = columnDescriptors.length;
        M metadata = batch.getMetadata();
        for (T tuple : batch) {
            adapter.startRow(out, numColumns);
            for (int columnIndex = 0, projectedIndex = 0; columnIndex < numColumns; columnIndex++) {
                ColumnDescriptor columnDescriptor = columnDescriptors[columnIndex];

                if (!columnDescriptor.isProjected()) {
                    adapter.writeNull(out);
                    continue;
                }

                DataType dataType = columnDescriptor.getDataType();

                adapter.startField(out);
                switch (dataType) {
                    case BIGINT: {
                        writeLong(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case BOOLEAN: {
                        writeBoolean(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case BPCHAR:
                    case TEXT:
                    case VARCHAR: {
                        writeText(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case BYTEA: {
                        writeBytes(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case DATE: {
                        writeDate(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case FLOAT8: {
                        writeDouble(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case INTEGER: {
                        writeInteger(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case NUMERIC: {
                        writeNumeric(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case REAL: {
                        writeFloat(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case SMALLINT: {
                        writeShort(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    case TIMESTAMP: {
                        writeTimestamp(out, tuple, projectedIndex, metadata, adapter);
                        break;
                    }

                    default:
                        throw new UnsupportedTypeException("Unsupported data type " + dataType);
                }
                adapter.endField(out);
                projectedIndex++;
            }
            adapter.endRow(out);
        }
    }

    @Override
    public void close(OutputStream out, SerializerAdapter adapter) throws IOException {
        adapter.close(out);
    }

    protected abstract void writeLong(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeBoolean(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeText(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeBytes(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeDouble(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeInteger(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeFloat(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeShort(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeDate(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeTimestamp(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;

    protected abstract void writeNumeric(OutputStream out, T tuple, int columnIndex, M metadata, SerializerAdapter adapter) throws IOException;
}
