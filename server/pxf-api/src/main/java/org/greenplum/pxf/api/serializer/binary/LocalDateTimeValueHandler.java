package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.serializer.converter.LocalDateTimeConverter;
import org.greenplum.pxf.api.serializer.converter.ValueConverter;

import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;

public class LocalDateTimeValueHandler extends BaseBinaryValueHandler<Object> {

    private final ValueConverter<LocalDateTime, Long> dateTimeConverter;

    public LocalDateTimeValueHandler() {
        this(new LocalDateTimeConverter());
    }

    public LocalDateTimeValueHandler(ValueConverter<LocalDateTime, Long> dateTimeConverter) {

        this.dateTimeConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutput buffer, final Object value) throws IOException {
        buffer.writeInt(8);

        LocalDateTime localDateTime;
        if (value instanceof LocalDateTime) {
            localDateTime = (LocalDateTime) value;
        } else {
            localDateTime = LocalDateTime.parse(value.toString(), GreenplumDateTime.DATETIME_FORMATTER);
        }
        buffer.writeLong(dateTimeConverter.convert(localDateTime));
    }
}
