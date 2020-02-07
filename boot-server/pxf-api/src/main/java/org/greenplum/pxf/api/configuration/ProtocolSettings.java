package org.greenplum.pxf.api.configuration;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class ProtocolSettings {

    /**
     * The name of the underlying protocol
     */
    private String protocol;

    /**
     * The fully qualified class name of the protocol handler
     */
    private String handler;

    /**
     * A map of option mappings
     */
    private Map<String, String> optionMappings;
}
