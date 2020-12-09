package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(DataOutputStream buffer, String value) throws IOException {
        writeString(buffer, value);
    }
}
