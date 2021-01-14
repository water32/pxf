package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all {@link DataSplitter} types. Initializes the splitter
 * with the query session.
 */
public abstract class BaseDataSplitter implements DataSplitter {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Configuration configuration;
    protected QuerySession<?, ?> querySession;
    protected RequestContext context;

    /**
     * Constructs a {@link DataSplitter} and initializes the {@link Plugin}
     *
     * @param querySession the query session
     */
    public BaseDataSplitter(QuerySession<?, ?> querySession) {
        this.querySession = querySession;
        this.context = querySession.getContext();
        this.configuration = context.getConfiguration();
    }
}
