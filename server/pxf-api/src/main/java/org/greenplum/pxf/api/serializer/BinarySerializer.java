package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.serializer.binary.BinaryValueHandlerProvider;

import java.io.IOException;
import java.io.OutputStream;

public class BinarySerializer extends BaseSerializer {

    private final ValueHandlerProvider valueHandlerProvider;

    public BinarySerializer() {
        this.valueHandlerProvider = new BinaryValueHandlerProvider();
    }

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
    }

    @Override
    public <T> void writeField(DataType dataType, T field) throws IOException {
        valueHandlerProvider.resolve(dataType).handle(buffer, field);
    }

    @Override
    public void endField() {
    }

    @Override
    public void endRow() {
    }

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
}
