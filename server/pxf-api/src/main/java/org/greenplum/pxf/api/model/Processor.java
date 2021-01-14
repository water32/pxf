package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.serializer.TupleSerializer;

import java.io.IOException;

/**
 * Interface that defines tuple access from the external data store (HDFS,
 * JDBC, Hive, Cloud Stores, etc) and tuple resolution.
 *
 * @param <T> the tuple type that the processor returns
 */
public interface Processor<T, M> {

    /**
     * Returns the query splitter for this {@link Processor}
     *
     * @param querySession the session for the query
     * @return the query splitter
     */
    DataSplitter getDataSplitter(QuerySession<T, M> querySession);

    /**
     * Process the current split and return an iterator to retrieve tuples
     * from the external system.
     *
     * @param querySession the session for the query
     * @param split        the split
     * @return an iterator of tuples of type {@code T}
     */
    TupleIterator<T, M> getTupleIterator(QuerySession<T, M> querySession, DataSplit split) throws IOException;

    TupleSerializer<T, M> tupleSerializer(QuerySession<T, M> querySession);

    /**
     * Returns true if this processor can handle the request, false otherwise
     *
     * @param querySession the session for the query
     * @return true if this processor can handle the request, false otherwise
     */
    boolean canProcessRequest(QuerySession<T, M> querySession);
}
