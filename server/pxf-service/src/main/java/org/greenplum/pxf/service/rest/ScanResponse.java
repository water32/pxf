package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleBatch;
import org.greenplum.pxf.api.serializer.TupleSerializer;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Serializes a response for a foreign scan request to the output stream. The
 * {@link QuerySession} object is used to propagate state of the query, and
 * it contains the {@code outputQueue} where tuple batches are stored for
 * consumption and serialization.
 */
public class ScanResponse<T, M> implements StreamingResponseBody {

    private static final Logger LOG = LoggerFactory.getLogger(ScanResponse.class);

    private final int segmentId;
    private final QuerySession<T, M> querySession;
    private final RequestContext context;
    private final List<ColumnDescriptor> columnDescriptors;

    /**
     * Constructs a new {@link ScanResponse} object with the given
     * {@code querySession}.
     *
     * @param querySession the query session object
     */
    public ScanResponse(int segmentId, QuerySession<T, M> querySession) {
        this.segmentId = segmentId;
        this.querySession = querySession;
        this.context = querySession.getContext();
        this.columnDescriptors = context.getTupleDescription();
    }

    /**
     * Writes the tuples resulting from the foreign scan to the {@code output}
     * stream. It uses the appropriate serialization format requested from the
     * request context.
     *
     * @param output the output stream
     */
    @SneakyThrows
    @Override
    public void writeTo(OutputStream output) {

        LOG.debug("{}-{}-- Starting streaming for {}", context.getTransactionId(),
                segmentId, querySession);

        int tupleCount = 0;
        SerializerAdapter adapter = querySession.getAdapter();
        TupleSerializer<T, M> serializer = querySession.getProcessor().tupleSerializer(querySession);
        BlockingQueue<TupleBatch<T, M>> outputQueue = querySession.getOutputQueue();
        ColumnDescriptor[] columnDescriptors = this.columnDescriptors
                .stream()
                .filter(ColumnDescriptor::isProjected)
                .toArray(ColumnDescriptor[]::new);

        int pollTimeout = context.getConfiguration()
                .getInt("pxf.response.poll-timeout", 5);

        Instant start;
        long tupleCountForStats = 0;

        try {
            serializer.open(output, adapter);

            start = Instant.now();
            while (querySession.isActive()) {
                TupleBatch<T, M> batch = outputQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);

                if (!querySession.isActive()) {
                    // Double check again to make sure that the query is still active
                    break;
                }

                if (batch == null) {
                    // keep waiting until the query session becomes inactive or
                    // there are items in the output queue to be consumed
                    continue;
                }

                // serialize a batch of tuples to the output stream using the
                // adapter for the query

                serializer.serialize(output, columnDescriptors, batch, adapter);
                tupleCount += batch.size();
                tupleCountForStats += batch.size();

                batch.clear();

                if (tupleCountForStats >= 2_000) {
                    querySession.reportConsumptionStats(segmentId, tupleCountForStats, Duration.between(start, Instant.now()).toNanos());
                    tupleCountForStats = 0;
                    start = Instant.now();
                }
            }

            if (!querySession.isQueryErrored() && !querySession.isQueryCancelled()) {
                /*
                 * We only close the serializer when there are no errors in the
                 * query execution, otherwise, we will flush the buffer to the
                 * client and close the connection. When an error occurs we
                 * need to discard the buffer, and replace it with an error
                 * response and a new error code.
                 */
                serializer.close(output, adapter);
            } else {
                querySession.ensureErrorReported();
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery();
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.warn(String.format("Remote connection closed by Greenplum (segment %s)", segmentId), e);
            } else {
                LOG.warn("Remote connection closed by Greenplum (segment {}) (Enable debug for stacktrace)", segmentId);
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            querySession.errorQuery(e, true);
            LOG.error("Error while writing data for segment {}", segmentId, e);
            throw e;
        } catch (Exception e) {
            querySession.errorQuery(e, true);
            LOG.error("Error while writing data for segment {}", segmentId, e);
            throw new IOException(e.getMessage(), e);
        } finally {
            querySession.deregisterSegment(tupleCount);
            LOG.debug("{}-{}-- Stopped streaming {} record{} for {}",
                    context.getTransactionId(), segmentId,
                    tupleCount, tupleCount == 1 ? "" : "s", querySession);
        }
    }
}
