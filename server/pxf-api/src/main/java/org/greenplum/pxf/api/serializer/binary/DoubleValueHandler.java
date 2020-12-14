package org.greenplum.pxf.api.serializer.binary;

import java.io.DataOutput;
import java.io.IOException;

public class DoubleValueHandler<T extends Number> extends BaseBinaryValueHandler<T> {

    @Override
    protected void internalHandle(DataOutput buffer, final T value) throws IOException {
        buffer.writeInt(8);
        buffer.writeDouble(value.doubleValue());
    }
}
