package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.function.TriFunction;

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
    DataSplitter getDataSplitter(QuerySession<T> querySession);

    /**
     * Process the current split and return an iterator to retrieve tuples
     * from the external system.
     *
     * @param querySession the session for the query
     * @param split        the split
     * @return an iterator of tuples of type {@code T}
     */
    TupleIterator<T> getTupleIterator(QuerySession<T> querySession, DataSplit split) throws IOException;

//    /**
//     * Return a list of fields for the the tuple
//     *
//     * @param querySession the session for the query
//     * @param tuple        the tuple
//     * @return the list of fields for the given tuple
//     */
//    Iterator<Object> getFields(QuerySession<T> querySession, T tuple) throws IOException;

    /**
     * Returns an array of mapping functions that map a tuple at a given index
     * to the resolved type to be serialized.
     *
     * @param querySession the session for the query
     * @return the array of mapping functions
     */
    TriFunction<QuerySession<T>, T, Integer, Object>[] getMappingFunctions(QuerySession<T> querySession);

    /**
     * Returns true if this processor can handle the request, false otherwise
     *
     * @param querySession the session for the query
     * @return true if this processor can handle the request, false otherwise
     */
    boolean canProcessRequest(QuerySession<T> querySession);
}
