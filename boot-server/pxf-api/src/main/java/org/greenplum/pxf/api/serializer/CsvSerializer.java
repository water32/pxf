package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.serializer.csv.CsvValueHandlerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class CsvSerializer extends BaseSerializer {

    private final String delimiter;
    private final GreenplumCSV greenplumCSV;
    private final ValueHandlerProvider valueHandlerProvider;
    private final ValueHandler<String> stringValueHandler;
    private int currentField;

    public CsvSerializer(GreenplumCSV greenplumCSV) {
        this.greenplumCSV = requireNonNull(greenplumCSV, "greenpplumCSV is null");
        this.valueHandlerProvider = new CsvValueHandlerProvider();
        this.stringValueHandler = valueHandlerProvider.resolve(DataType.TEXT);
        this.delimiter = String.valueOf(greenplumCSV.getDelimiter());
    }

    @Override
    public void startRow(int numColumns) {
        currentField = 0;
    }

    @Override
    public void startField() throws IOException {
        if (currentField > 0 && greenplumCSV.getDelimiter() != null) {
            stringValueHandler.handle(buffer, delimiter);
        }
    }

    @Override
    public <T> void writeField(DataType dataType, T field) throws IOException {
        currentField++;
        if (field == null) {
            stringValueHandler.handle(buffer, greenplumCSV.getValueOfNull());
        } else {
            // TODO: best effort to resolve the field to the greenplum datatype
            if (DataType.isTextForm(dataType.getOID()) && !DataType.isArrayType(dataType.getOID()) && field instanceof String) {
                stringValueHandler.handle(buffer,
                        greenplumCSV.toCsvField((String) field, true, true, true));
            } else {
                valueHandlerProvider.resolve(dataType).handle(buffer, field);
            }
        }
    }

    @Override
    public void endField() {
    }

    @Override
    public void endRow() throws IOException {
        stringValueHandler.handle(buffer, greenplumCSV.getNewline());
    }
}
