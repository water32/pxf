package org.greenplum.pxf.api.serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class BinaryTupleSerializer<T> extends BaseTupleSerializer<T> {

    @Override
    public void open(DataOutputStream out) throws IOException {
        // 11 bytes required header
        out.writeBytes("PGCOPY\n\377\r\n\0");
        // 32 bit integer indicating no OID
        out.writeInt(0);
        // 32 bit header extension area length
        out.writeInt(0);
    }

    @Override
    public void startRow(DataOutputStream out, int numColumns) throws IOException {
        // Each tuple begins with a 16-bit integer count of the number of
        // fields in the tuple
        out.write((numColumns >>> 8) & 0xFF);
        out.write((numColumns >>> 0) & 0xFF);
    }

    @Override
    public void startField(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void endField(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void endRow(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void close(DataOutputStream out) throws IOException {
        // The file trailer consists of a 16-bit integer word containing -1.
        // This is easily distinguished from a tuple's field-count word.
        out.writeShort(-1);
    }

    protected void writeNull(DataOutputStream out) throws IOException {
        // As a special case, -1 indicates a NULL field value.
        // No value bytes follow in the NULL case.
        out.writeInt(-1);
    }

    protected void doWriteLong(DataOutputStream out, long value) throws IOException {
        out.writeInt(8);
        out.writeLong(value);
    }

    protected void doWriteBoolean(DataOutputStream out, boolean value) throws IOException {
        out.writeInt(1);
        out.writeByte(value ? 1 : 0);
    }

    protected void doWriteText(DataOutputStream out, String value) throws IOException {
        doWriteText(out, value, StandardCharsets.UTF_8);
    }

    protected void doWriteText(DataOutputStream out, String value, Charset charset) throws IOException {
        doWriteText(out, value.getBytes(charset));
    }

    protected void doWriteText(DataOutputStream out, byte[] value) throws IOException {
        int length = value.length;
        out.write((length >>> 24) & 0xFF);
        out.write((length >>> 16) & 0xFF);
        out.write((length >>> 8) & 0xFF);
        out.write((length >>> 0) & 0xFF);
        out.write(value, 0, length);
    }
}
