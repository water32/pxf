package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(DataOutputStream buffer, Object value) throws IOException {
        writeString(buffer, value.toString());
    }
}
