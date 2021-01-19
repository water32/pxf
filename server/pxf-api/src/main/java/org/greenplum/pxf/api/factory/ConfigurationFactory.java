package org.greenplum.pxf.api.factory;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONFIG_RESOURCE_PATH_PROPERTY = "pxf.config.resource.path";

    /**
     * Configuration property that stores the server directory
     */
    String PXF_CONFIG_SERVER_DIRECTORY_PROPERTY = "pxf.config.server.directory";

    /**
     * Configuration property that stores the server name
     */
    String PXF_SERVER_NAME_PROPERTY = "pxf.config.server.name";

    /**
     * Synthetic configuration property that stores the user so that is can be
     * used in config files for interpolation in other properties, for example
     * in JDBC when setting session authorization from a proxy user to the
     * end-user
     */
    String PXF_SESSION_USER_PROPERTY = "pxf.session.user";

    /**
     * Name of the property that allows overriding the number of maximum
     * concurrent threads that process tuples
     */
    String PXF_PROCESSOR_SCALE_FACTOR_PROPERTY = "pxf.processor.scale-factor";

    /**
     * Name of the property that allows overriding the default batch size
     */
    String PXF_PROCESSOR_BATCH_SIZE_PROPERTY = "pxf.processor.batch-size";

    /**
     * Name of the property that allows overriding the queue size of the processor queue
     */
    String PXF_PROCESSOR_QUEUE_SIZE_PROPERTY = "pxf.processor.queue-size";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param configDirectory name of the configuration directory
     * @param serverName name of the server
     * @param userName name of the user
     * @param additionalProperties additional properties to be added to the configuration
     * @return configuration object
     */
    Configuration initConfiguration(String configDirectory, String serverName, String userName, Map<String, String> additionalProperties);
}
