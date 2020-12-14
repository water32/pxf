package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutput;
import java.io.IOException;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(DataOutput buffer, String value) throws IOException {
        writeString(buffer, value);
    }
}
