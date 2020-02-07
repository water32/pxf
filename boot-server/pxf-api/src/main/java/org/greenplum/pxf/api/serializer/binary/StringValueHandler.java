package org.greenplum.pxf.api.serializer.binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringValueHandler extends BaseBinaryValueHandler<String> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws IOException {
        byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);

        buffer.writeInt(utf8Bytes.length);
        buffer.write(utf8Bytes);
    }
}