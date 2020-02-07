package org.greenplum.pxf.api.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;

/**
 * A query split holds information about a data split.
 * {@link DataSplitter} iterates over splits for a query slice.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
@AllArgsConstructor
public class DataSplit {

    /**
     * File path+name, table name, etc.
     */
    @Getter
    private final String resource;

    /**
     * Split metadata information (starting point + length, region location, etc.).
     */
    @Getter
    @Setter
    private byte[] metadata;

    /**
     * ThirdParty data added to a fragment. Ignored if null.
     */
    @Getter
    @Setter
    private byte[] userData;

    /**
     * Constructs a DataSplit.
     *
     * @param resource the resource uri (file path+name, table name, etc.)
     * @param metadata the meta data (starting point + length, region location, etc.).
     */
    public DataSplit(String resource, byte[] metadata) {
        this(resource, metadata, null);
    }

    /**
     * Returns a unique resource name for the given split
     *
     * @return a unique resource name for the given split
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(resource);
        if (metadata != null) {
            sb.append(":").append(Hex.encodeHex(metadata));
        }
        if (userData != null) {
            sb.append(":").append(Hex.encodeHex(userData));
        }
        return sb.toString();
    }
}
