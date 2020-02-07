package org.greenplum.pxf.api.factory;

import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Serializer;
import org.greenplum.pxf.api.serializer.BinarySerializer;
import org.greenplum.pxf.api.serializer.CsvSerializer;
import org.springframework.stereotype.Component;

@Component
public class SerializerFactory {

    public Serializer getSerializer(RequestContext context) {
        if (context.getOutputFormat() == OutputFormat.TEXT) {
            return new CsvSerializer(context.getGreenplumCSV());
        } else if (context.getOutputFormat() == OutputFormat.Binary) {
            return new BinarySerializer();
        }

        throw new UnsupportedOperationException(String.format("The output format '%s' is not supported", context.getOutputFormat()));
    }
}
