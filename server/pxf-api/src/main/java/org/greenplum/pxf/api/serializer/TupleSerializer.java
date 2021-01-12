package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;

public interface TupleSerializer<T> {

    void open(final DataOutputStream out) throws IOException;

    void serialize(final DataOutputStream out, ColumnDescriptor[] columnDescriptors, T tuple) throws IOException;

    void close(final DataOutputStream out) throws IOException;
}
