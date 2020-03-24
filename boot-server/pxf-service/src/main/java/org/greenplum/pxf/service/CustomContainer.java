package org.greenplum.pxf.service;

import org.apache.coyote.http11.Http11NioProtocol;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class CustomContainer implements
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    @Autowired
    private PxfServerProperties serverProperties;

    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            Http11NioProtocol handler = (Http11NioProtocol) connector.getProtocolHandler();
            handler.setMaxHeaderCount(serverProperties.getTomcat().getMaxHeaderCount());
        });
    }
}
