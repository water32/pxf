package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.serializer.converter.LocalDateTimeConverter;
import org.greenplum.pxf.api.serializer.converter.ValueConverter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;

public class LocalDateTimeValueHandler extends BaseBinaryValueHandler<LocalDateTime> {

    private ValueConverter<LocalDateTime, Long> dateTimeConverter;

    public LocalDateTimeValueHandler() {
        this(new LocalDateTimeConverter());
    }

    public LocalDateTimeValueHandler(ValueConverter<LocalDateTime, Long> dateTimeConverter) {

        this.dateTimeConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final LocalDateTime value) throws IOException {
        buffer.writeInt(8);
        buffer.writeLong(dateTimeConverter.convert(value));
    }
}
