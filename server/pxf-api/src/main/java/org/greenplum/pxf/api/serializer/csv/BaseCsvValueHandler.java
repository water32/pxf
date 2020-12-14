package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class BaseCsvValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(DataOutput buffer, T value) throws IOException {
        internalHandle(buffer, value);
    }

    protected abstract void internalHandle(DataOutput buffer, final T value)
            throws IOException;

    protected void writeString(DataOutput buffer, String value) throws IOException {
        buffer.write(value.getBytes(StandardCharsets.UTF_8));
    }
}
