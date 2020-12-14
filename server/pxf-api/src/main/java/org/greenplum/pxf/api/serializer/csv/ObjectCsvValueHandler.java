package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutput;
import java.io.IOException;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(DataOutput buffer, Object value) throws IOException {
        writeString(buffer, value.toString());
    }
}
