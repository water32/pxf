package org.greenplum.pxf.api.model;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterates over the {@link DataSplit}s for a given segment. It uses a
 * consistent hashing algorithm to determine which segment will process a
 * given split. The {@link FragmentMetadata} object in the {@link DataSplit}
 * needs to implement the {@code hashCode()} method to allow the consistent
 * hashing algorithm to always produce the same hash value for a given
 * {@link DataSplit}.
 */
public class DataSplitSegmentIterator<S extends DataSplit> implements Iterator<S> {

    /**
     * The hash function to use to determine whether the segment will process
     * a given split. {@link Hashing#crc32()} is the hashing function that
     * was more fairly distributing the workload among segments.
     */
    private static final HashFunction HASH_FUNCTION = Hashing.crc32();

    private final Iterator<S> iterator;
    private final int totalSegments;
    private final int segmentId;
    private S next = null;

    /**
     * Constructs a new {@link DataSplitSegmentIterator} for a given
     * {@code segmentId}, with {@code totalSegments}, and a {@code collection}
     * of elements.
     *
     * @param segmentId     the segment identifier for the iterator
     * @param totalSegments the total number of segments
     * @param collection    a collection of elements
     */
    public DataSplitSegmentIterator(int segmentId, int totalSegments, Collection<S> collection) {
        this(segmentId, totalSegments, collection.iterator());
    }

    /**
     * Constructs a new {@link DataSplitSegmentIterator} for a given
     * {@code segmentId}, with {@code totalSegments}, and the {@code iterator}.
     *
     * @param segmentId     the segment identifier for the iterator
     * @param totalSegments the total number of segments
     * @param iterator      the original iterator
     */
    public DataSplitSegmentIterator(int segmentId, int totalSegments, Iterator<S> iterator) {
        this.segmentId = segmentId;
        this.totalSegments = totalSegments;
        this.iterator = iterator;
    }

    /**
     * Returns {@code true} if the iteration has more elements for a given
     * {@code segmentId}.
     *
     * @return {@code true} if the iteration has more elements for the given {@code segmentId}
     */
    @Override
    public boolean hasNext() {
        ensureNextItemIsReady();
        return next != null;
    }

    /**
     * Returns the next element for the given {@code segmentId} in the iteration.
     *
     * @return the next element for the given {@code segmentId} in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public S next() {
        ensureNextItemIsReady();
        if (next == null)
            throw new NoSuchElementException();
        S value = next;
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
            hasher = hasher.putInt(split.hashCode());
        }
        return segmentId == Hashing.consistentHash(hasher.hash(), totalSegments);
    }

    /**
     * Makes sure the {@code next} element is populated if any is available
     */
    private void ensureNextItemIsReady() {
        if (next == null && iterator.hasNext()) {
            S n = iterator.next();
            while (!doesSegmentProcessThisSplit(n) && iterator.hasNext()) {
                n = iterator.next();
            }
            if (doesSegmentProcessThisSplit(n)) {
                next = n;
            }
        }
    }
}
