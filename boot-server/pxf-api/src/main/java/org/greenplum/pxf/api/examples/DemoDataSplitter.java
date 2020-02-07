package org.greenplum.pxf.api.examples;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BaseDataSplitter;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.RequestContext;

import java.util.NoSuchElementException;

/**
 * Class that defines the splitting of a data resource into splits that can
 * be processed in parallel. next() returns the split information of a given
 * path (resource and location of each split).
 *
 * <p>Demo implementation
 */
public class DemoDataSplitter extends BaseDataSplitter {
    private static final int TOTAL_FRAGMENTS = 3;
    private int currentFragment = 1;

    /**
     * Constructs a DemoDataSplitter
     *
     * @param context       the context for the request
     * @param configuration the server configuration
     */
    public DemoDataSplitter(RequestContext context, Configuration configuration) {
        super(context, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return currentFragment <= TOTAL_FRAGMENTS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplit next() {
        if (currentFragment > TOTAL_FRAGMENTS)
            throw new NoSuchElementException();

        DataSplit split = new DataSplit(context.getDataSource() + "." + currentFragment,
            ("fragment" + currentFragment).getBytes());
        currentFragment++;
        return split;
    }
}
