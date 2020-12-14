package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;

public class DateCsvValueHandler extends BaseCsvValueHandler<Date> {

    @Override
    protected void internalHandle(DataOutput buffer, Date value) throws IOException {
        writeString(buffer, value.toString());
    }
}
