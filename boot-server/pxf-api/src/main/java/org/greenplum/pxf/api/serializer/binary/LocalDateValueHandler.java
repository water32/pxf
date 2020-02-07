package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.serializer.converter.LocalDateConverter;
import org.greenplum.pxf.api.serializer.converter.ValueConverter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;

public class LocalDateValueHandler extends BaseBinaryValueHandler<String> {

    private ValueConverter<LocalDate, Integer> dateConverter;

    public LocalDateValueHandler() {
        this(new LocalDateConverter());
    }

    public LocalDateValueHandler(ValueConverter<LocalDate, Integer> dateTimeConverter) {

        this.dateConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws IOException {
        buffer.writeInt(4);
        buffer.writeInt(dateConverter.convert(LocalDate.parse(value)));
    }
}