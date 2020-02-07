package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class BaseBinaryValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(DataOutputStream buffer, final T value) throws IOException {
        if (value == null) {
            buffer.writeInt(-1);
        } else {
            internalHandle(buffer, value);
        }
    }

    protected abstract void internalHandle(DataOutputStream buffer, final T value) throws IOException;
}