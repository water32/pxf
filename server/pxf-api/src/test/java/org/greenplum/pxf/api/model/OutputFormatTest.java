package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OutputFormatTest {

    @Test
    void testUnsupportedOutputFormat() {
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> OutputFormat.getOutputFormat("foo"));
        assertThat(e.getMessage()).isEqualTo("Unable to find output format by given class name: foo");
    }

    @Test
    void testGetTextOutputFormat() {
        OutputFormat actual = OutputFormat.getOutputFormat("org.greenplum.pxf.api.io.Text");
        assertThat(actual).isEqualTo(OutputFormat.TEXT);
        assertThat(actual.name()).isEqualTo("TEXT");
        assertThat(actual.getClassName()).isEqualTo("org.greenplum.pxf.api.io.Text");
    }

    @Test
    void testGetGPDBWritableOutputFormat() {
        OutputFormat actual = OutputFormat.getOutputFormat("org.greenplum.pxf.api.io.GPDBWritable");
        assertThat(actual).isEqualTo(OutputFormat.GPDBWritable);
        assertThat(actual.name()).isEqualTo("GPDBWritable");
        assertThat(actual.getClassName()).isEqualTo("org.greenplum.pxf.api.io.GPDBWritable");
    }

    @Test
    void testGetBinaryOutputFormat() {
        OutputFormat actual = OutputFormat.getOutputFormat("org.greenplum.pxf.api.io.Binary");
        assertThat(actual).isEqualTo(OutputFormat.BINARY);
        assertThat(actual.name()).isEqualTo("BINARY");
        assertThat(actual.getClassName()).isEqualTo("org.greenplum.pxf.api.io.Binary");
    }

}
