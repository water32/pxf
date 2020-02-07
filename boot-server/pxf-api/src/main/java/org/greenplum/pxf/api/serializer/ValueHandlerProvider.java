package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;

public interface ValueHandlerProvider {

    <T> ValueHandler<T> resolve(DataType dataType);
}
