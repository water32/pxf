package org.greenplum.pxf.api.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PxfServerProperties.class)
public class PxfConfiguration {
}
