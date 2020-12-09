package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.GreenplumDateTime;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Timestamp;

public class TimeStampCsvValueHandler extends BaseCsvValueHandler<Timestamp> {

    @Override
    protected void internalHandle(DataOutputStream buffer, Timestamp value) throws IOException {
        writeString(buffer, value.toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER));
    }
}