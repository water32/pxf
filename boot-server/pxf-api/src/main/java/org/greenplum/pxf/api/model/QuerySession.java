package org.greenplum.pxf.api.model;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.task.ProducerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 *
 * @param <T> the type of the tuple that is returned by the query
 */
public class QuerySession<T, M> {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySession.class);

    private final AtomicBoolean queryCancelled;
    private final AtomicBoolean queryErrored;
    private final AtomicInteger activeSegments;
    private final AtomicBoolean finishedProducing;
    private final AtomicLong totalRecordCount;

    private final String queryId;
    private final Instant startTime;
    private final int totalSegments;
    private Instant endTime;
    private Instant cancelTime;

    private Instant errorTime;

    private UserGroupInformation effectiveUgi;

    /**
     * The list of splits for the query. The list of splits is only set when
     * the "Fragmenter" cache is enabled
     */
    @Getter
    @Setter
    private volatile List<DataSplit> dataSplitList;

    private final BlockingDeque<List<List<Object>>> outputQueue;

    private final BlockingDeque<Processor<T>> processorQueue;

    private final Deque<Exception> errors;

    @Getter
    @Setter
    private volatile M metadata;

    /**
     * Tracks number of active tasks
     */
    final AtomicInteger createdTasks = new AtomicInteger();

    /**
     * Tracks number of active tasks
     */
    final AtomicInteger completedTasks = new AtomicInteger();

    public QuerySession(String queryId, int totalSegments) {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.startTime = Instant.now();
        this.queryCancelled = new AtomicBoolean(false);
        this.queryErrored = new AtomicBoolean(false);
        this.finishedProducing = new AtomicBoolean(false);
        this.activeSegments = new AtomicInteger(0);
        this.outputQueue = new LinkedBlockingDeque<>(200);
        this.totalRecordCount = new AtomicLong(0);
        this.processorQueue = new LinkedBlockingDeque<>();
        this.totalSegments = totalSegments;
        this.errors = new ConcurrentLinkedDeque<>();

        ProducerTask<T, M> producer = new ProducerTask<>(this);
        producer.setName("pxf-producer-" + queryId);
        producer.start();

        LOG.info("Created {}", this);
    }

    public BlockingDeque<List<List<Object>>> getOutputQueue() {
        return outputQueue;
    }

    /**
     * Cancels the query, the first thread to cancel the query sets the cancel
     * time
     */
    public void cancelQuery(ClientAbortException e) {
        if (!queryCancelled.getAndSet(true)) {
            cancelTime = Instant.now();
        }
        errors.offer(e);
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
     * Marks the query as errored, the first thread to error the query sets
     * the error time
     */
    public void errorQuery(Exception e) {
        if (!queryErrored.getAndSet(true)) {
            errorTime = Instant.now();
        }
        errors.offer(e);
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
     * Registers a query splitter to the session
     *
     * @param processor the processor
     */
    public void registerProcessor(Processor<T> processor) throws InterruptedException {
        if (!isActive()) {
            LOG.debug("Skip registering processor because the query session is no longer active");
            return;
        }

        activeSegments.incrementAndGet();
        processorQueue.put(processor);
    }

    /**
     * Deregisters a segment, and the last segment to deregister the endTime
     * is recorded.
     *
     * @param recordCount the recordCount from the segment
     */
    public void deregisterSegment(long recordCount) {

        totalRecordCount.addAndGet(recordCount);

        if (activeSegments.decrementAndGet() == 0) {
            endTime = Instant.now();
            outputQueue.clear(); // unblock producers in the case of error
            dataSplitList = null;

            long totalRecords = totalRecordCount.get();
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
     * Determines whether the query session is active. The query session
     * becomes inactive if the query is errored or the query is cancelled.
     *
     * @return true if the query is active, false when the query has errors or is cancelled
     */
    public boolean isActive() {
        return !isQueryErrored() && !isQueryCancelled();
    }

    public int getCreatedTasks() {
        return createdTasks.get();
    }

    public void registerTask() {
        createdTasks.incrementAndGet();
    }

    public void registerCompletedTask() {
        completedTasks.incrementAndGet();
    }

    public int getCompletedTasks() {
        return completedTasks.get();
    }

    public void markAsFinishedProducing() {
        finishedProducing.set(true);
    }

    public boolean hasFinishedProducing() {
        return finishedProducing.get();
    }

    public BlockingDeque<Processor<T>> getProcessorQueue() {
        return processorQueue;
    }

    public Deque<Exception> getErrors() {
        return errors;
    }

    public int getTotalSegments() {
        return totalSegments;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuerySession<?, ?> that = (QuerySession<?, ?>) o;
        return Objects.equal(queryId, that.queryId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(queryId);
    }
}
