package org.greenplum.pxf.api.factory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public abstract class BasePluginFactory<T extends Plugin> implements PluginFactory<T> {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * {@inheritDoc}
     */
    @Override
    public T getPlugin(RequestContext context, Configuration configuration) {

        // get the class name of the plugin
        String pluginClassName = getPluginClassName(context);
        if (StringUtils.isBlank(pluginClassName)) {
            throw new RuntimeException("Could not determine plugin class name");
        }

        // load the class by name
        Class<?> cls;
        try {
            cls = Class.forName(pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Class %s is not found", pluginClassName), e);
        }

        // check if the class is a plugin
        if (!Plugin.class.isAssignableFrom(cls)) {
            throw new RuntimeException(String.format("Class %s does not implement Plugin interface", pluginClassName));
        }

        // create plugin instance
        Plugin instance;
        try {
            instance = (Plugin) applicationContext.getBean(cls);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Class %s could not be instantiated", pluginClassName), e);
        }

        // initialize the instance
        instance.initialize(context, configuration);

        // cast into a target type
        @SuppressWarnings("unchecked")
        T castInstance = (T) instance;

        return castInstance;
    }

    /**
     * Returns the class name of the plugin from the given context
     *
     * @param context the context for the request
     * @return the class name of the plugin from the given context
     */
    abstract protected String getPluginClassName(RequestContext context);
}
