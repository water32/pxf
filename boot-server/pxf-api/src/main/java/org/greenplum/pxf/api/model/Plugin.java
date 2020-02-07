package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

/**
 * Base interface for all plugin types that manages initialization and provides
 * information on plugin thread safety
 */
public interface Plugin {

    /**
     * Initialize the plugin for the incoming request
     *
     * @param context       data provided in the request
     * @param configuration the server configuration
     */
    void initialize(RequestContext context, Configuration configuration);

    /**
     * Checks if the plugin is thread safe
     *
     * @return true if plugin is thread safe, false otherwise
     */
    boolean isThreadSafe();
}
