package org.greenplum.pxf.service.spring;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.springframework.boot.web.embedded.jetty.JettyWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PxfJettyInitializationListener implements ApplicationListener<ServletWebServerInitializedEvent> {
    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
        WebServer webServer = event.getWebServer();
        if (webServer instanceof JettyWebServer) {
            Server server = ((JettyWebServer) webServer).getServer();
            log.info("Jetty Server ={}", server);
            log.info("- port       ={}", webServer.getPort());
        }
    }
}
