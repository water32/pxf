package org.greenplum.pxf.api.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.catalina.connector.ClientAbortException;

import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 */
@EqualsAndHashCode(of = {"queryId"})
@ToString(of = {"queryId"})
public class QuerySession {

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
     * True if the query has been cancelled, false otherwise
     */
    private final AtomicBoolean queryCancelled;

    /**
     * Records the Instant when the query was cancelled, null if the query was
     * not cancelled
     */
    private Instant cancelTime;

    /**
     * A queue of the errors encountered during processing of this query
     * session.
     */
    @Getter
    private final Deque<Exception> errors;

    /**
     * The queue used to process tuples.
     */
    @Getter
    private final BlockingDeque<List<List<Object>>> outputQueue;

    public QuerySession(RequestContext context) {
        this.context = context;
        this.queryId = String.format("%s:%s:%s:%s", context.getServerName(),
                context.getTransactionId(), context.getDataSource(), context.getFilterString());
        this.queryCancelled = new AtomicBoolean(false);
        this.outputQueue = new LinkedBlockingDeque<>(200);
        this.errors = new ConcurrentLinkedDeque<>();
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
}
