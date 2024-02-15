package org.greenplum.pxf.service.spring;

import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * The {@link PxfJettyCustomizer} class allows customizing application container
 * properties that are not exposed through the application.properties file.
 */
@Component
public class PxfJettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {

    private final PxfServerProperties pxfServerProperties;

    /** Create a new PxfJettyCustomizer with the given server properties
     *
     * @param pxfServerProperties the server properties
     */
    public PxfJettyCustomizer(PxfServerProperties pxfServerProperties) {
        this.pxfServerProperties = pxfServerProperties;
    }

    @Override
    public void customize(JettyServletWebServerFactory factory) {

    }
}
