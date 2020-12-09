package org.greenplum.pxf.api.serializer;

import java.io.DataOutputStream;
import java.io.IOException;

public interface ValueHandler<T> {

    void handle(DataOutputStream buffer, final T value) throws IOException;
}
