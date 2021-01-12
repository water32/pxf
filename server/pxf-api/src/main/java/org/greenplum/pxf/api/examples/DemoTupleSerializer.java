package org.greenplum.pxf.api.examples;

import org.greenplum.pxf.api.serializer.BinaryTupleSerializer;
import org.springframework.stereotype.Component;

import java.io.DataOutputStream;
import java.io.IOException;

@Component
public class DemoTupleSerializer extends BinaryTupleSerializer<String[]> {
    @Override
    protected void writeLong(DataOutputStream out, String[] tuple, int columnIndex) {

    }

    @Override
    protected void writeBoolean(DataOutputStream out, String[] tuple, int columnIndex) {

    }

    @Override
    protected void writeText(DataOutputStream out, String[] tuple, int columnIndex) throws IOException {
        doWriteText(out, tuple[columnIndex]);
    }
}
