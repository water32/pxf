package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.io.DataType;

import java.io.IOException;
import java.io.OutputStream;

public interface Serializer extends AutoCloseable {

    void open(final OutputStream out) throws IOException;

    void startRow(int numColumns) throws IOException;

    void startField() throws IOException;

    <T> void writeField(DataType dataType, T field) throws IOException;

    void endField() throws IOException;

    void endRow() throws IOException;

    void close() throws IOException, RuntimeException;
}
