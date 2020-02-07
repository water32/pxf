package org.greenplum.pxf.api.factory;

import org.greenplum.pxf.api.PxfTestConfig;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Serializer;
import org.greenplum.pxf.api.serializer.BinarySerializer;
import org.greenplum.pxf.api.serializer.CsvSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = PxfTestConfig.class)
@WebAppConfiguration
class SerializerFactoryTest {

    @Autowired
    private SerializerFactory serializerFactory;

    @Test
    public void testTextOutputFormatSerializer() {
        RequestContext context = new RequestContext();
        context.setOutputFormat(OutputFormat.TEXT);

        Serializer serializer = serializerFactory.getSerializer(context);
        assertNotNull(serializer);
        assertEquals(CsvSerializer.class, serializer.getClass());
    }

    @Test
    public void testBinaryOutputFormatSerializer() {
        RequestContext context = new RequestContext();
        context.setOutputFormat(OutputFormat.Binary);

        Serializer serializer = serializerFactory.getSerializer(context);
        assertNotNull(serializer);
        assertEquals(BinarySerializer.class, serializer.getClass());
    }

    @Test
    public void testGpdbWritableOutputFormatSerializer() {
        RequestContext context = new RequestContext();
        context.setOutputFormat(OutputFormat.GPDBWritable);

        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> serializerFactory.getSerializer(context));

        assertEquals("The output format 'GPDBWritable' is not supported", exception.getMessage());
    }

}
