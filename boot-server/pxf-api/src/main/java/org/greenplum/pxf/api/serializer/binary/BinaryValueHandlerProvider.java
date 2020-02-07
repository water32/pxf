package org.greenplum.pxf.api.serializer.binary;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.serializer.ValueHandler;
import org.greenplum.pxf.api.serializer.ValueHandlerProvider;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BinaryValueHandlerProvider implements ValueHandlerProvider {

    private final Map<DataType, ValueHandler> valueHandlers;

    public BinaryValueHandlerProvider() {
        StringValueHandler stringCsvValueHandler = new StringValueHandler();

        valueHandlers = new EnumMap<>(DataType.class);
        valueHandlers.put(DataType.BIGINT, new LongValueHandler<>());
        valueHandlers.put(DataType.BOOLEAN, new BooleanValueHandler());
        valueHandlers.put(DataType.BPCHAR, stringCsvValueHandler);
        valueHandlers.put(DataType.BYTEA, new ByteArrayValueHandler());
        valueHandlers.put(DataType.DATE, new LocalDateValueHandler());
        valueHandlers.put(DataType.FLOAT8, new DoubleValueHandler<>());
        valueHandlers.put(DataType.INTEGER, new IntegerValueHandler<>());
        valueHandlers.put(DataType.NUMERIC, new BigDecimalValueHandler<>());
        valueHandlers.put(DataType.REAL, new FloatValueHandler<>());
        valueHandlers.put(DataType.SMALLINT, new ShortValueHandler<>());
        valueHandlers.put(DataType.TEXT, stringCsvValueHandler);
        valueHandlers.put(DataType.TIMESTAMP, new LocalDateTimeValueHandler());
        valueHandlers.put(DataType.VARCHAR, stringCsvValueHandler);
    }

    @Override
    public <T> ValueHandler<T> resolve(DataType dataType) {
        @SuppressWarnings("unchecked")
        ValueHandler<T> handler = valueHandlers.get(dataType);
        if (handler == null) {
            throw new IllegalArgumentException(String.format("DataType '%s' has not been registered", dataType));
        }
        return handler;
    }

    @Override
    public String toString() {
        String valueHandlersString = valueHandlers.values()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));

        return "BinaryValueHandlerProvider{" +
                "valueHandlers=" + valueHandlersString +
                '}';
    }
}
