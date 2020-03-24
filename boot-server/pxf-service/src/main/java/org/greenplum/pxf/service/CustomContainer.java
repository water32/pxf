package org.greenplum.pxf.service;

import org.apache.coyote.http11.Http11NioProtocol;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class CustomContainer implements
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    private final PxfServerProperties serverProperties;

    public CustomContainer(PxfServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            Http11NioProtocol handler = (Http11NioProtocol) connector.getProtocolHandler();
            PxfServerProperties.Tomcat tomcatProperties = serverProperties.getTomcat();

            handler.setMaxHeaderCount(tomcatProperties.getMaxHeaderCount());
            handler.setMaxHttpHeaderSize(tomcatProperties.getMaxHeaderSize());
        });
    }
}
