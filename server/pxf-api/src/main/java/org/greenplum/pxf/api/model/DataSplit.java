package org.greenplum.pxf.api.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

/**
 * A query split holds information about a data split.
 * {@link DataSplitter} iterates over splits for a query slice.
 */
@EqualsAndHashCode
@AllArgsConstructor
public class DataSplit {

    /**
     * File path+name, table name, etc.
     */
    @Getter
    private final String resource;

    /**
     * Fragment metadata information (starting point + length, region location, etc.).
     */
    @Getter
    private final FragmentMetadata metadata;

    /**
     * Returns a unique resource name for the given split
     *
     * @return a unique resource name for the given split
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(resource);
        if (metadata != null) {
            sb.append(":").append(metadata.toString());
        }
        return sb.toString();
    }
}
