package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.function.TriFunction;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.QuerySession;

import java.io.IOException;
import java.io.OutputStream;

public interface Serializer extends AutoCloseable {

    void open(final OutputStream out) throws IOException;

    void startRow(int numColumns) throws IOException;

    void startField() throws IOException;

    <T> void writeField(DataType dataType, 
                        TriFunction<QuerySession<T>, T, Integer, Object> function,
                        QuerySession<T> session,
                        T tuple,
                        int columnIndex) throws IOException;

    void endField() throws IOException;

    void endRow() throws IOException;

    void close() throws IOException, RuntimeException;
}
