package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.greenplum.pxf.api.serializer.BaseTupleSerializer;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;

import java.io.IOException;
import java.io.OutputStream;

public class AvroTupleSerializer extends BaseTupleSerializer<AvroWrapper<GenericRecord>, Schema> {
    @Override
    protected void writeLong(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {
        
    }

    @Override
    protected void writeBoolean(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeText(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeBytes(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeDouble(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeInteger(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeFloat(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeShort(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeDate(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeTimestamp(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }

    @Override
    protected void writeNumeric(OutputStream out, AvroWrapper<GenericRecord> tuple, int columnIndex, Schema metadata, SerializerAdapter adapter) throws IOException {

    }
}
