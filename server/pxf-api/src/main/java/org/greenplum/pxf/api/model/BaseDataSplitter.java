package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all {@link DataSplitter} types. Initializes the
 * {@link Plugin} with the given {@link RequestContext} and
 * {@link Configuration}
 */
public abstract class BaseDataSplitter implements DataSplitter {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Configuration configuration;
    protected RequestContext context;

    /**
     * Constructs a {@link DataSplitter} and initializes the {@link Plugin}
     *
     * @param context the request context for the given query
     */
    public BaseDataSplitter(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
    }
}
