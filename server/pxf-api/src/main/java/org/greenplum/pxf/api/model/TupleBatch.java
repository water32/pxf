package org.greenplum.pxf.api.model;

import java.util.ArrayList;

public class TupleBatch<T, M> extends ArrayList<T> {

    private final M metadata;

    public TupleBatch(int capacity, M metadata) {
        super(capacity);
        this.metadata = metadata;
    }

    public M getMetadata() {
        return metadata;
    }
}
