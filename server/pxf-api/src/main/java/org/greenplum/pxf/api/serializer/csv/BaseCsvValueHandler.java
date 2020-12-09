package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class BaseCsvValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(DataOutputStream buffer, T value) throws IOException {
        internalHandle(buffer, value);
    }

    protected abstract void internalHandle(DataOutputStream buffer, final T value)
            throws IOException;

    protected void writeString(DataOutputStream buffer, String value) throws IOException {
        buffer.write(value.getBytes(StandardCharsets.UTF_8));
    }
}
