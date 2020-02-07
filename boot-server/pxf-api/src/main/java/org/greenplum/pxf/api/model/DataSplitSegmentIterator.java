package org.greenplum.pxf.api.model;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DataSplitSegmentIterator implements Iterator<DataSplit> {

    private static final HashFunction HASH_FUNCTION = Hashing.crc32();

    private final Iterator<DataSplit> iterator;
    private final int totalSegments;
    private final int segmentId;
    private DataSplit next = null;

    public DataSplitSegmentIterator(int segmentId, int totalSegments, Iterator<DataSplit> iterator) {
        this.segmentId = segmentId;
        this.totalSegments = totalSegments;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (next == null && iterator.hasNext()) {
            DataSplit n = iterator.next();
            while (!doesSegmentProcessThisSplit(n) && iterator.hasNext()) {
                n = iterator.next();
            }
            if (doesSegmentProcessThisSplit(n)) {
                next = n;
            }
        }
        return next != null;
    }

    @Override
    public DataSplit next() {
        if (next == null)
            throw new NoSuchElementException();
        DataSplit value = next;
        next = null;
        return value;
    }

    /**
     * Determine whether this thread will handle the split. To determine
     * which thread should process an element at a given index I for the source
     * SOURCE_NAME, use a CONSISTENT_HASH function
     * <p>
     * S = CONSISTENT_HASH(hash(SOURCE_NAME[:META_DATA][:USER_DATA]), N)
     *
     * <p>This hash function is deterministic for a given SOURCE_NAME, and allows
     * the same thread processing for segment S to always process the same
     * source. This allows for caching the Fragment at the segment S, as
     * segment S is guaranteed to always process the same split.
     *
     * @param split the split
     * @return true if the thread handles the split, false otherwise
     */
    protected boolean doesSegmentProcessThisSplit(DataSplit split) {
        Hasher hasher = HASH_FUNCTION.newHasher()
                .putString(split.getResource(), StandardCharsets.UTF_8);
        if (split.getMetadata() != null) {
            hasher = hasher.putBytes(split.getMetadata());
        }
        if (split.getUserData() != null) {
            hasher = hasher.putBytes(split.getUserData());
        }
        return segmentId == Hashing.consistentHash(hasher.hash(), totalSegments);
    }
}
