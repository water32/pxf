package org.greenplum.pxf.api.serializer.csv;

import org.apache.commons.codec.binary.Hex;

import java.io.DataOutput;
import java.io.IOException;

public class ByteArrayCsvValueHandler extends BaseCsvValueHandler<byte[]> {

    @Override
    protected void internalHandle(DataOutput buffer, byte[] value) throws IOException {
        writeString(buffer, "\\x");
        writeString(buffer, Hex.encodeHexString(value));
    }
}
