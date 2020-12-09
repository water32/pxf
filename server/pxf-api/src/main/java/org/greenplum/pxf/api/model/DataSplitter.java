package org.greenplum.pxf.api.model;

import java.util.Iterator;

/**
 * Interface that defines how data should be split, and returns an iterator of
 * {@link DataSplit}s.
 */
public interface DataSplitter extends Iterator<DataSplit>, Plugin{
}
