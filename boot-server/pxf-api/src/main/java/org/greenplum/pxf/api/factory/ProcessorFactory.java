package org.greenplum.pxf.api.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.examples.DemoProcessor;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.RequestContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ProcessorFactory extends BasePluginFactory<Processor<?>> {

    private final ApplicationContext applicationContext;

    public ProcessorFactory(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Processor<?> getPlugin(RequestContext context, Configuration configuration) {
        Map<String, Processor> processorMap = applicationContext.getBeansOfType(Processor.class);

        Processor<?> processor = processorMap
            .values()
            .stream()
            .filter(p -> p.canProcessRequest(context))
            .findFirst()
            .orElseThrow(() -> {
                String errorMessage;
                if (StringUtils.isBlank(context.getFormat())) {
                    errorMessage = String.format("There are no registered processors to handle the '%s' protocol", context.getProtocol());
                } else {
                    errorMessage = String.format("There are no registered processors to handle the '%s' protocol and '%s' format", context.getProtocol(), context.getFormat());
                }
                return new IllegalArgumentException(errorMessage);
            });
        processor.initialize(context, configuration);
        return processor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getPluginClassName(RequestContext context) {
        throw new UnsupportedOperationException();
    }
}
