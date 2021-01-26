package org.greenplum.pxf.api.examples;

import org.greenplum.pxf.api.serializer.BaseTupleSerializer;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

@Component
public class DemoTupleSerializer extends BaseTupleSerializer<String[], Void> {

    @Override
    protected void writeLong(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeBoolean(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeText(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter, Charset encoding) throws IOException {
        adapter.writeText(out, tuple[columnIndex], encoding);
    }

    @Override
    protected void writeBytes(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeDouble(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeInteger(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeFloat(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeShort(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeDate(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeTimestamp(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING

    @Override
    protected void writeNumeric(OutputStream out, String[] tuple, int columnIndex, Void metadata, SerializerAdapter adapter) {
    } // DO NOTHING
}
