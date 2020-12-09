package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.serializer.converter.LocalDateConverter;
import org.greenplum.pxf.api.serializer.converter.ValueConverter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;

public class LocalDateValueHandler extends BaseBinaryValueHandler<Object> {

    private ValueConverter<LocalDate, Integer> dateConverter;

    public LocalDateValueHandler() {
        this(new LocalDateConverter());
    }

    public LocalDateValueHandler(ValueConverter<LocalDate, Integer> dateTimeConverter) {

        this.dateConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final Object value) throws IOException {
        buffer.writeInt(4);
        if (value instanceof Date) {
            buffer.writeInt(dateConverter.convert(((Date) value).toLocalDate()));
        } else {
            buffer.writeInt(dateConverter.convert(LocalDate.parse(value.toString())));
        }
    }
}
