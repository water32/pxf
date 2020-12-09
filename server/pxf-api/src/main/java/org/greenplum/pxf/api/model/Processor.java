package org.greenplum.pxf.api.model;

import java.io.IOException;
import java.util.Iterator;

/**
 * Interface that defines tuple access from the external data store (HDFS,
 * JDBC, Hive, Cloud Stores, etc) and tuple resolution.
 *
 * @param <T> the tuple type that the processor returns
 */
public interface Processor<T> {

    /**
     * Returns the query splitter for this {@link Processor}
     *
     * @param querySession the session for the query
     * @return the query splitter
     */
    DataSplitter getDataSplitter(QuerySession querySession);

    /**
     * Process the current split and return an iterator to retrieve tuples
     * from the external system.
     *
     * @param querySession the session for the query
     * @param split        the split
     * @return an iterator of tuples of type {@code T}
     */
    TupleIterator<T> getTupleIterator(QuerySession querySession, DataSplit split) throws IOException;

    /**
     * Return a list of fields for the the tuple
     *
     * @param tuple the tuple
     * @return the list of fields for the given tuple
     */
    Iterator<Object> getFields(T tuple) throws IOException;

    /**
     * Returns true if this processor can handle the request, false otherwise
     *
     * @param context the context for the request
     * @return true if this processor can handle the request, false otherwise
     */
    boolean canProcessRequest(RequestContext context);
}
