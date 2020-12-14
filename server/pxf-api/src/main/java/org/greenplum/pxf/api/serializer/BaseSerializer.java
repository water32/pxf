package org.greenplum.pxf.api.serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseSerializer implements Serializer, AutoCloseable {

    protected transient DataOutputStream buffer;

    @Override
    public void open(final OutputStream out) throws IOException {
        buffer = new DataOutputStream(out);
    }

    @Override
    public void close() throws IOException {
        buffer.flush();
        buffer.close();
    }
}
