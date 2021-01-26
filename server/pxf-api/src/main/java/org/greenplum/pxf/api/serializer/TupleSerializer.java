package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.model.TupleBatch;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public interface TupleSerializer<T, M> {

    void open(OutputStream out, SerializerAdapter adapter) throws IOException;

    void serialize(OutputStream out, ColumnDescriptor[] columnDescriptors, TupleBatch<T, M> batch, SerializerAdapter adapter, Charset encoding) throws IOException;

    void close(OutputStream out, SerializerAdapter adapter) throws IOException;

}
