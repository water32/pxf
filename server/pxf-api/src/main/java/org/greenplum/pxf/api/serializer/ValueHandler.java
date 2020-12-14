package org.greenplum.pxf.api.serializer;

import java.io.DataOutput;
import java.io.IOException;

public interface ValueHandler<T> {

    void handle(DataOutput buffer, final T value) throws IOException;
}
