package org.greenplum.pxf.api.serializer.binary;

import java.io.DataOutput;
import java.io.IOException;

public class ByteArrayValueHandler extends BaseBinaryValueHandler<byte[]> {

    @Override
    protected void internalHandle(DataOutput buffer, final byte[] value) throws IOException {
        buffer.writeInt(value.length);
        buffer.write(value, 0, value.length);
    }
}
