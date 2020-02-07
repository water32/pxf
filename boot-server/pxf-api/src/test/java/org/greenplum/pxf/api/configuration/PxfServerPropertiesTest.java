package org.greenplum.pxf.api.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@EnableConfigurationProperties(value = PxfServerProperties.class)
@TestPropertySource("classpath:server-config-test.properties")
class PxfServerPropertiesTest {

    @Autowired
    PxfServerProperties properties;

    @Test
    public void testPxfConfIsSet() {
        assertNotNull(properties.getConf());
        assertEquals("/path/to/pxf/conf", properties.getConf());
        assertNotNull(properties.getProtocolSettings());

        assertEquals(1, properties.getProtocolSettings().size());

        ProtocolSettings s3ProtocolSettings;
        assertNotNull((s3ProtocolSettings = properties.getProtocolSettings().get("s3")));

        assertEquals("s3a", s3ProtocolSettings.getProtocol());
        assertEquals("org.greenplum.pxf.plugins.s3.S3ProtocolHandler", s3ProtocolSettings.getHandler());
        assertNotNull(s3ProtocolSettings.getOptionMappings());
        assertEquals(2, s3ProtocolSettings.getOptionMappings().size());

        assertEquals("fs.s3a.access.key", s3ProtocolSettings.getOptionMappings().get("accesskey"));
        assertEquals("fs.s3a.secret.key", s3ProtocolSettings.getOptionMappings().get("secretkey"));
    }
}
