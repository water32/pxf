package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntArrayCsvValueHandler extends BaseCsvValueHandler<int[]> {

    @Override
    protected void internalHandle(DataOutputStream buffer, int[] value) throws IOException {

        writeString(buffer, "{");
        for (int i = 0; i < value.length; i++) {
            writeString(buffer, Integer.toString(value[i]));
            if (i < value.length - 1) {
                writeString(buffer, ",");
            }
        }
        writeString(buffer, "}");
    }
}
