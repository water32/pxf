package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.serializer.BaseTupleSerializer;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;

@Component
public class AvroTupleSerializer extends BaseTupleSerializer<GenericRecord, Schema> {

    @Override
    protected void writeLong(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeLong(out, (long) tuple.get(columnIndex));
    }

    @Override
    protected void writeBoolean(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeBoolean(out, (boolean) tuple.get(columnIndex));
    }

    @Override
    protected void writeText(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeBytes(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeBytes(out, (byte[]) tuple.get(columnIndex));
    }

    @Override
    protected void writeDouble(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeDouble(out, (double) tuple.get(columnIndex));
    }

    @Override
    protected void writeInteger(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeInteger(out, (int) tuple.get(columnIndex));
    }

    @Override
    protected void writeFloat(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeFloat(out, (float) tuple.get(columnIndex));
    }

    @Override
    protected void writeShort(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        adapter.writeFloat(out, (short) tuple.get(columnIndex));
    }

    @Override
    protected void writeDate(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeTimestamp(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeNumeric(OutputStream out, GenericRecord tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }
}
