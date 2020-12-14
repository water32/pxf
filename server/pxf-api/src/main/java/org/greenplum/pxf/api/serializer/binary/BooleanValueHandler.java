package org.greenplum.pxf.api.serializer.binary;

import java.io.DataOutput;
import java.io.IOException;

public class BooleanValueHandler extends BaseBinaryValueHandler<Boolean> {

    @Override
    protected void internalHandle(DataOutput buffer, final Boolean value) throws IOException {
        buffer.writeInt(1);
        buffer.writeByte(value ? 1 : 0);
    }
}
