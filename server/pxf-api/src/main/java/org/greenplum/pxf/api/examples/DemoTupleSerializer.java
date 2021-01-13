package org.greenplum.pxf.api.examples;

import org.greenplum.pxf.api.serializer.BinaryTupleSerializer;
import org.springframework.stereotype.Component;

import java.io.DataOutputStream;
import java.io.IOException;

@Component
public class DemoTupleSerializer extends BinaryTupleSerializer<String[]> {

    @Override
    protected void writeLong(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeBoolean(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeText(DataOutputStream out, String[] tuple, int columnIndex) throws IOException {
        doWriteText(out, tuple[columnIndex]);
    }

    @Override
    protected void writeBytes(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeDouble(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeInteger(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeFloat(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeShort(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeDate(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeTimestamp(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING

    @Override
    protected void writeNumeric(DataOutputStream out, String[] tuple, int columnIndex) {
    } // DO NOTHING
}
