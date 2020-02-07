package org.greenplum.pxf.api.factory;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;

/**
 * Factory interface for getting instances of the plugins based on their class names.
 *
 * @param <T> interface that the resulting plugin should implement
 */
public interface PluginFactory<T extends Plugin> {

    /**
     * Get an instance of the plugin with a given class names.
     *
     * @param context       context of the current request
     * @param configuration the server configuration
     * @return an initialized instance of the plugin
     */
    T getPlugin(RequestContext context, Configuration configuration);
}
