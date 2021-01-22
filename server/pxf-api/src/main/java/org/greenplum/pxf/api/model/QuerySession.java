package org.greenplum.pxf.api.model;

import com.google.common.cache.Cache;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.greenplum.pxf.api.serializer.adapter.BinarySerializerAdapter;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.greenplum.pxf.api.factory.ConfigurationFactory.PXF_PROCESSOR_QUEUE_SIZE_PROPERTY;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 */
@EqualsAndHashCode(of = {"queryId"})
public class QuerySession<T, M> {

    private static final int DEFAULT_PROCESSOR_QUEUE_SIZE = 20;

    private static final Logger LOG = LoggerFactory.getLogger(QuerySession.class);

    /**
     * Synchronization lock for registration/de-registration access.
     */
    private final ReentrantLock registrationLock;

    /**
     * Wait until there are no active segments
     */
    private final Condition noActiveSegments;

    /**
     * A unique identifier for the query
     */
    @Getter
    private final String queryId;

    /**
     * The request context for the given query
     */
    @Getter
    private final RequestContext context;

    /**
     * Meter registry for reporting custom metrics
     */
    @Getter
    private final MeterRegistry meterRegistry;

    /**
     * The binary serializer adapter
     */
    @Getter
    private final SerializerAdapter adapter;

    /**
     * The processor used for this query session
     */
    @Getter
    @Setter
    private Processor<T, M> processor;

    /**
     * True if the query is active, false otherwise
     */
    private final AtomicBoolean queryActive;

    /**
     * True if the query has been cancelled, false otherwise
     */
    private final AtomicBoolean queryCancelled;

    /**
     * True if the query has errors, false otherwise
     */
    private final AtomicBoolean queryErrored;

    /**
     * True if the error has been reported to the client, false otherwise
     */
    private final AtomicBoolean errorReported;

    /**
     * Records the Instant when the query was created
     */
    private final Instant startTime;

    /**
     * Records the Instant when the query was cancelled, null if the query was
     * not cancelled
     */
    private Instant cancelTime;

    /**
     * Records the Instant when the first error occurred in the query, null
     * if there are no errors
     */
    private Instant errorTime;

    /**
     * A queue of the errors encountered during processing of this query
     * session.
     */
    @Getter
    private final Deque<Exception> errors;

    /**
     * A queue of segments that have registered to this query session
     */
    @Getter
    private final BlockingDeque<Integer> registeredSegmentQueue;

    /**
     * The queue used to process tuples.
     */
    @Getter
    private final BlockingQueue<TupleBatch<T, M>> outputQueue;

    /**
     * Number of active segments that have registered to this QuerySession
     */
    private int activeSegmentCount;

    /**
     * Tracks number of created tasks
     */
    private final AtomicInteger createdProcessorTaskCount;

    /**
     * Tracks number of completed tasks
     */
    private final AtomicInteger completedProcessorTaskCount;

    /**
     * The total number of tuples that were streamed out to the client
     */
    private long totalTupleCount;

    /**
     * Holds a reference to the querySessionCache
     */
    private final Cache<String, QuerySession<T, M>> querySessionCache;

    /**
     *
     */
    private final DescriptiveStatistics stats;

    public QuerySession(
            RequestContext context,
            Cache<String, QuerySession<T, M>> querySessionCache,
            MeterRegistry meterRegistry) {
        this.context = context;
        this.querySessionCache = querySessionCache;
        this.meterRegistry = meterRegistry;
        this.adapter = resolveSerializationAdapter(context);
        this.registrationLock = new ReentrantLock();
        this.noActiveSegments = registrationLock.newCondition();
        this.queryId = String.format("%s:%s:%s:%s", context.getServerName(),
                context.getTransactionId(), context.getDataSource(), context.getFilterString());
        this.startTime = Instant.now();
        this.queryActive = new AtomicBoolean(true);
        this.queryCancelled = new AtomicBoolean(false);
        this.queryErrored = new AtomicBoolean(false);
        this.errorReported = new AtomicBoolean(false);
        this.registeredSegmentQueue = new LinkedBlockingDeque<>();

        int processorQueueSize = context.getConfiguration()
                .getInt(PXF_PROCESSOR_QUEUE_SIZE_PROPERTY, DEFAULT_PROCESSOR_QUEUE_SIZE);

        this.outputQueue = new LinkedBlockingQueue<>(processorQueueSize);
        this.errors = new ConcurrentLinkedDeque<>();
        this.activeSegmentCount = 0;
        this.createdProcessorTaskCount = new AtomicInteger(0);
        this.completedProcessorTaskCount = new AtomicInteger(0);
        this.totalTupleCount = 0;
//        this.rateTableInMs = new double[context.getTotalSegments()];
//        Arrays.fill(rateTableInMs, -1);

        // Create a DescriptiveStats instance and set the window size to 1000
        stats = new DescriptiveStatistics(1000);
    }

    /**
     * Registers a segment to this query session
     *
     * @param segmentId the segment identifier
     */
    public void registerSegment(int segmentId) throws InterruptedException {
        final ReentrantLock registrationLock = this.registrationLock;
        registrationLock.lock();
        try {
            if (!isActive()) {
                throw new IllegalStateException("This querySession is no longer active.");
            }

            activeSegmentCount++;
            registeredSegmentQueue.put(segmentId);
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * Attempt to mark this {@link QuerySession} as inactive. The operation
     * will succeed only if there are no registered segments in the queue.
     * Otherwise, a new segment was able to register to the query session
     * right before we were about to mark this session as inactive.
     */
    public void tryMarkInactive() {
        final ReentrantLock registrationLock = this.registrationLock;
        registrationLock.lock();
        try {
            if (registeredSegmentQueue.isEmpty() && outputQueue.isEmpty()) {

                String cacheKey = String.format("%s:%s:%s:%s",
                        context.getServerName(), context.getTransactionId(),
                        context.getDataSource(), context.getFilterString());

                // Remove from cache
                querySessionCache.invalidate(cacheKey);

                // This query session is no longer active when all the segments
                // have de-registered, this means all the segments have completed
                // streaming data to the output stream
                queryActive.set(false);
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * De-registers a segment.
     *
     * @param recordCount the recordCount from the segment
     */
    public void deregisterSegment(long recordCount) {
        final ReentrantLock registrationLock = this.registrationLock;
        registrationLock.lock();
        try {
            totalTupleCount += recordCount;

            if (--activeSegmentCount == 0) {
                noActiveSegments.signal();
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * Waits, if there are still active segments, until all segments have
     * de-registered.
     */
    public void waitForAllSegmentsToDeregister() throws InterruptedException {
        final ReentrantLock registrationLock = this.registrationLock;
        registrationLock.lock();
        try {
            if (activeSegmentCount > 0) {
                noActiveSegments.await();
            }
        } finally {
            registrationLock.unlock();
        }
    }

    /**
     * Cancels the query, the first thread to cancel the query sets the cancel
     * time
     */
    public void cancelQuery() {
        queryActive.set(false);
        if (!queryCancelled.getAndSet(true)) {
            cancelTime = Instant.now();
        }
    }

    /**
     * Marks the query as errored, the first thread to error the query sets
     * the error time
     *
     * @param e        the error
     * @param reported whether the error has been reported to the client
     */
    public void errorQuery(Exception e, boolean reported) {
        queryActive.set(false);
        if (!queryErrored.getAndSet(true)) {
            errorTime = Instant.now();
        }
        errors.offer(e);
        if (reported) {
            errorReported.set(true);
        }
    }

    /**
     * Makes sure that the error is reported to the client.
     *
     * @throws Exception the error
     */
    public void ensureErrorReported() throws Exception {
        if (errorReported.compareAndSet(false, true) && !errors.isEmpty()) {
            throw errors.getFirst();
        }
    }

    /**
     * Returns the number of tasks that have completed
     *
     * @return the number of tasks that have completed
     */
    public int getCompletedProcessorCount() {
        return completedProcessorTaskCount.get();
    }

    /**
     * Returns the number of tasks that have been created
     *
     * @return the number of tasks that have been created
     */
    public int getCreatedProcessorCount() {
        return createdProcessorTaskCount.get();
    }

    /**
     * Determines whether the query session is active. The query session
     * becomes inactive if the query is errored, the query is cancelled,
     * or all the segments have completed streaming data to the output stream.
     *
     * @return true if the query is active, false when the query has errors, is cancelled, or all segments completed streaming data to the output stream
     */
    public boolean isActive() {
        return queryActive.get();
    }

    /**
     * Check whether the query has errors
     *
     * @return true if the query has errors, false otherwise
     */
    public boolean isQueryErrored() {
        return queryErrored.get();
    }

    /**
     * Check whether the query was cancelled
     *
     * @return true if the query was cancelled, false otherwise
     */
    public boolean isQueryCancelled() {
        return queryCancelled.get();
    }

    /**
     * Increments the number of created tasks
     */
    public void markTaskAsCreated() {
        createdProcessorTaskCount.incrementAndGet();
    }

    /**
     * Increments the number of completed tasks
     */
    public void markTaskAsCompleted() {
        completedProcessorTaskCount.incrementAndGet();
    }

    /**
     * Cleans up queues, and reports statistics on this {@link QuerySession}
     */
    public void close() {

        Instant endTime = Instant.now();

        // Clear the output queue in case of error or cancellation
        outputQueue.clear();

        // Clear the queue of registered segments
        registeredSegmentQueue.clear();

        // TODO: Close UGI

        if (cancelTime != null) {

            long durationMs = Duration.between(startTime, cancelTime).toMillis();
            LOG.info("{} canceled after {}ms", this, durationMs);

        } else if (errorTime != null) {

            long durationMs = Duration.between(startTime, errorTime).toMillis();
            LOG.info("{} errored after {}ms", this, durationMs);

        } else {

            long durationMs = Duration.between(startTime, endTime).toMillis();
            double rate = durationMs == 0 ? 0 : (1000.0 * totalTupleCount / durationMs);

            LOG.info("{} completed streaming {} tuple{} in {}ms. {} tuples/sec",
                    this,
                    totalTupleCount,
                    totalTupleCount == 1 ? "" : "s",
                    durationMs,
                    String.format("%.2f", rate));

        }
    }

    private SerializerAdapter resolveSerializationAdapter(RequestContext context) {
        switch (context.getOutputFormat()) {
            case TEXT:
                throw new UnsupportedOperationException();
            case BINARY:
                return SpringContext.getBean(BinarySerializerAdapter.class);
            default:
                throw new UnsupportedOperationException(
                        String.format("The output format '%s' is not supported", context.getOutputFormat()));
        }
    }

    private double mean;

    private final AtomicBoolean canAddWorker = new AtomicBoolean(false);
    private final AtomicBoolean canRemoveWorker = new AtomicBoolean(false);

    private Instant lastUpdateOfCondition = Instant.now();

    private volatile boolean changedInLastWindow = false;

    private long count = 0;

    public void reportConsumptionStats(int segmentId, long tupleCount, long durationNanos) {
        if (durationNanos == 0)
            return;

        long c = 0;
        double rateMs = (double) (tupleCount * 1_000_000) / (double) (durationNanos);
        double oldMean = 0;
        boolean hasSignificantIncrease = false;
        long timeSinceLastUpdate;

        synchronized (stats) {
            count++;
            stats.addValue(rateMs);

            // When enough time has elapsed, we make some decision about flipping
            // the boolean for allowing additional workers
            timeSinceLastUpdate = Duration.between(lastUpdateOfCondition, Instant.now()).toMillis();
            if (timeSinceLastUpdate > 5000) {
                oldMean = mean;
                mean = stats.getMean();
                lastUpdateOfCondition = Instant.now();

                hasSignificantIncrease = (mean - oldMean) / oldMean >= 0.2;

                if (hasSignificantIncrease) {
                    canAddWorker.set(true);
                } else if (changedInLastWindow) {
                    canRemoveWorker.set(true);
                }
                changedInLastWindow = false;
                c = count;
                count = 0;
                stats.clear();
            }
        }

        if (timeSinceLastUpdate > 5000) {
            LOG.error("{}: oldMean {}, mean {}, hasSignificantIncrease {}, called {} times",
                    queryId,
                    oldMean,
                    mean,
                    hasSignificantIncrease,
                    c);
        }
    }

    public boolean shouldAddProcessorThread() {
        // single thread calls this method
        if (canAddWorker.compareAndSet(true, false)) {
            changedInLastWindow = true;
            return true;
        }
        return false;
    }

    public boolean shouldRemoveProcessorThread() {
        return canRemoveWorker.compareAndSet(true, false);
    }
}
