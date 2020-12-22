package org.greenplum.pxf.api.model;

import com.google.common.cache.Cache;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.task.TupleReaderTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 */
@EqualsAndHashCode(of = {"queryId"})
public class QuerySession<T> {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySession.class);

    /**
     * Synchronization lock for registration/deregistration access.
     */
    private final Object registrationLock;

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

//    /**
//     * Meter registry for reporting custom metrics
//     */
//    @Getter
//    private final MeterRegistry meterRegistry;

    /**
     * The processor used for this query session
     */
    @Getter
    @Setter
    private Processor<T> processor;

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
    private final Queue<List<T>> outputQueue;

    /**
     * Number of active segments that have registered to this QuerySession
     */
    private final AtomicInteger activeSegmentCount;

    /**
     * Tracks number of active tasks
     */
    private final AtomicInteger createdTupleReaderTaskCount;

    /**
     * Tracks number of completed tasks
     */
    private final AtomicInteger completedTupleReaderTaskCount;

    /**
     * The total number of tuples that were streamed out to the client
     */
    private final AtomicLong totalTupleCount;

    /**
     * Holds a reference to the querySessionCache
     */
    private final Cache<String, QuerySession<T>> querySessionCache;

    private final Map<Integer, TupleReaderTask<T>> tupleReaderTaskMap;

    public QuerySession(RequestContext context, Cache<String, QuerySession<T>> querySessionCache) {
        this.context = context;
        this.querySessionCache = querySessionCache;
//        this.meterRegistry = meterRegistry;

        this.registrationLock = new Object();
        this.queryId = String.format("%s:%s:%s:%s", context.getServerName(),
                context.getTransactionId(), context.getDataSource(), context.getFilterString());
        this.startTime = Instant.now();
        this.queryActive = new AtomicBoolean(true);
        this.queryCancelled = new AtomicBoolean(false);
        this.queryErrored = new AtomicBoolean(false);
        this.registeredSegmentQueue = new LinkedBlockingDeque<>();
        this.outputQueue = new LinkedBlockingQueue<>(400);
        this.errors = new ConcurrentLinkedDeque<>();
        this.activeSegmentCount = new AtomicInteger(0);
        this.createdTupleReaderTaskCount = new AtomicInteger(0);
        this.completedTupleReaderTaskCount = new AtomicInteger(0);
        this.totalTupleCount = new AtomicLong(0);
        this.tupleReaderTaskMap = new HashMap<>();
    }

    /**
     * Cancels the query, the first thread to cancel the query sets the cancel
     * time
     */
    public void cancelQuery(ClientAbortException e) {
        queryActive.set(false);
        if (!queryCancelled.getAndSet(true)) {
            cancelTime = Instant.now();
        }
        errors.offer(e);
    }

    /**
     * Registers a segment to this query session
     *
     * @param segmentId the segment identifier
     */
    public void registerSegment(int segmentId) throws InterruptedException {
        synchronized (registrationLock) {
            if (!isActive()) {
                throw new IllegalStateException("This querySession is no longer active.");
            }

            activeSegmentCount.incrementAndGet();
            registeredSegmentQueue.put(segmentId);
        }
    }

    /**
     * Attempt to mark this {@link QuerySession} as inactive. The operation
     * will succeed only if there are no registered segments in the queue.
     * Otherwise, a new segment was able to register to the query session
     * right before we were about to mark this session as inactive.
     */
    public void tryMarkInactive() {
        synchronized (registrationLock) {
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
        }

    }


    /**
     * De-registers a segment.
     *
     * @param recordCount the recordCount from the segment
     */
    public void deregisterSegment(long recordCount) {
        totalTupleCount.addAndGet(recordCount);
        activeSegmentCount.decrementAndGet();
    }

    /**
     * Marks the query as errored, the first thread to error the query sets
     * the error time
     */
    public void errorQuery(Exception e) {
        queryActive.set(false);
        if (!queryErrored.getAndSet(true)) {
            errorTime = Instant.now();
        }
        errors.offer(e);
    }

    /**
     * Returns the number of active segments for this {@link QuerySession}
     *
     * @return the number of active segments for this {@link QuerySession}
     */
    public int getActiveSegmentCount() {
        return activeSegmentCount.get();
    }

    /**
     * Returns the number of tasks that have completed
     *
     * @return the number of tasks that have completed
     */
    public int getCompletedTupleReaderTaskCount() {
        return completedTupleReaderTaskCount.get();
    }

    /**
     * Returns the number of tasks that have been created
     *
     * @return the number of tasks that have been created
     */
    public int getCreatedTupleReaderTaskCount() {
        return createdTupleReaderTaskCount.get();
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
     * Registers the {@link TupleReaderTask} to the querySession and keeps
     * track of the number of tasks that have been created.
     *
     * @param taskNumber a unique number for the task
     * @param task       the {@link TupleReaderTask}
     */
    public void addTupleReaderTask(int taskNumber, TupleReaderTask<T> task) {
        createdTupleReaderTaskCount.incrementAndGet();
        synchronized (tupleReaderTaskMap) {
            tupleReaderTaskMap.put(taskNumber, task);
        }
    }

    /**
     * De-registers the {@link TupleReaderTask} with the given
     * {@code taskIndex} and keeps track of the number of tasks that have
     * completed. The completion count is regardless the task completed
     * successfully or with failures.
     *
     * @param taskNumber a unique number for the task
     * @param task       the {@link TupleReaderTask}
     */
    public void removeTupleReaderTask(int taskNumber, TupleReaderTask<T> task) {
        completedTupleReaderTaskCount.incrementAndGet();
        synchronized (tupleReaderTaskMap) {
            tupleReaderTaskMap.remove(taskNumber);
        }
    }

    /**
     * Cancels all the {@link TupleReaderTask}s that are still active for this
     * {@link QuerySession}
     */
    public void cancelAllTasks() {
        synchronized (tupleReaderTaskMap) {
            for (Map.Entry<Integer, TupleReaderTask<T>> entry : tupleReaderTaskMap.entrySet()) {
                try {
                    entry.getValue().interrupt();
                } catch (Throwable e) {
                    LOG.warn(String.format("Unable to interrupt task number %d (%s)", entry.getKey(), entry.getValue()), e);
                }
            }
            tupleReaderTaskMap.clear();
        }
    }

    /**
     * Cleans up queues, and reports statistics on this {@link QuerySession}
     */
    public void close() {

        // Clear the output queue in case of error or cancellation
//        outputQueue.clear();

        // Clear the queue of registered segments
        registeredSegmentQueue.clear();

        // Clear tasks in the list
        tupleReaderTaskMap.clear();

        Instant endTime = Instant.now();
        long totalRecords = totalTupleCount.get();

        if (errorTime != null) {

            long durationMs = Duration.between(startTime, errorTime).toMillis();
            LOG.info("{} errored after {}ms", this, durationMs);

        } else if (cancelTime != null) {

            long durationMs = Duration.between(startTime, cancelTime).toMillis();
            LOG.info("{} canceled after {}ms", this, durationMs);

        } else {

            long durationMs = Duration.between(startTime, endTime).toMillis();
            double rate = durationMs == 0 ? 0 : (1000.0 * totalRecords / durationMs);

            LOG.info("{} completed streaming {} tuple{} in {}ms. {} tuples/sec",
                    this,
                    totalRecords,
                    totalRecords == 1 ? "" : "s",
                    durationMs,
                    String.format("%.2f", rate));

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "QuerySession@" +
                Integer.toHexString(System.identityHashCode(this)) +
                "{queryId='" + queryId + '\'' +
                '}';
    }
}
