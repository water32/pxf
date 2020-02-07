package org.greenplum.pxf.api.model;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.SerializerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class BaseProcessor<T, M> extends BasePlugin implements Processor<T> {

    // TODO: make configurable
    private static final long expireAfterAccessDurationMillis = 5;

    /**
     * A factory for the query session cache
     */
    private static final Cache<String, QuerySession<?, ?>> OUTPUT_QUEUE_CACHE =
        CacheBuilder.newBuilder()
            .expireAfterAccess(expireAfterAccessDurationMillis, TimeUnit.MILLISECONDS)
            .build();

    /**
     * A factory for serializers
     */
    private SerializerFactory serializerFactory;

    /**
     * A query session shared among all segments participating in this query
     */
    protected QuerySession<T, M> querySession;

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void initialize(RequestContext context, Configuration configuration) {
        super.initialize(context, configuration);
        final String cacheKey = getCacheKey(context);
        try {
            querySession = (QuerySession<T, M>) getQuerySession(cacheKey, context);
            LOG.debug("Registering new processor {} to querySession {}", this, querySession);
            querySession.registerProcessor(this);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the processed fragments to the output stream in the desired
     * wire output format
     *
     * @param output the output stream
     * @throws IOException             when an IOException occurs
     * @throws WebApplicationException when a WebApplicationException occurs
     */
    @Override
    public void writeTo(OutputStream output) throws IOException, WebApplicationException {
        int recordCount = 0;

        LOG.debug("{}-{}-- Starting streaming for {}", context.getTransactionId(), context.getSegmentId(), querySession);

        BlockingDeque<List<List<Object>>> outputQueue = querySession.getOutputQueue();
        try {
            Serializer serializer = serializerFactory.getSerializer(context);
            serializer.open(output);

            while (querySession.isActive()) {
                List<List<Object>> fieldList = outputQueue.poll(100, TimeUnit.MILLISECONDS);
                if (fieldList != null) {
                    for (List<Object> fields : fieldList) {
                        writeTuple(serializer, fields);
                        recordCount++;
                    }
                } else {
                    if (querySession.hasFinishedProducing()
                        && (querySession.getCompletedTasks() == querySession.getCreatedTasks())
                        && outputQueue.isEmpty()) {
                        break;
                    }
                }
            }

            if (querySession.isActive()) {
                /*
                 * We only close the serializer when there are no errors in the
                 * query execution, otherwise, we will flush the buffer to the
                 * client and close the connection. When an error occurs we
                 * need to discard the buffer, and replace it with an error
                 * page and a new error code.
                 */
                serializer.close();
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery(e);
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by client", e);
            } else {
                LOG.error("{}-{}: {}-- Remote connection closed by client (Enable debug for stacktrace)", context.getTransactionId(),
                    context.getSegmentId(), context.getDataSource());
            }
        } catch (Exception e) {
            querySession.errorQuery(e);
            LOG.error(e.getMessage() != null ? e.getMessage() : "ERROR", e);
            throw new IOException(e.getMessage(), e);
        } finally {
            querySession.deregisterSegment(recordCount);
            LOG.debug("{}-{}-- Stopped streaming {} record{} for {}",
                context.getTransactionId(), context.getSegmentId(), recordCount,
                recordCount == 1 ? "" : "s", querySession);
        }

        if (!querySession.isActive()) {
            Optional<Exception> firstException = querySession.getErrors().stream()
                .filter(e -> !(e instanceof ClientAbortException))
                .findFirst();

            if (firstException.isPresent()) {
                Exception e = firstException.get();
                throw new IOException(e.getMessage(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSegmentId() {
        return context.getSegmentId();
    }

    /**
     * Injects the serializerFactory instance
     *
     * @param serializerFactory the serializerFactory
     */
    @Autowired
    public final void setSerializerFactory(SerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
    }

    /**
     * Write the tuple to the serializer. The method retrieves an array of
     * fields for the given tuple and serializes each field using information
     * from the column descriptor to determine the type of the field
     *
     * @param serializer the serializer for the output format
     * @param fields     the list of fields
     * @throws IOException when an IOException occurs
     */
    protected void writeTuple(Serializer serializer, List<Object> fields) throws IOException {
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        serializer.startRow(tupleDescription.size());

        for (int i = 0; i < tupleDescription.size(); i++) {
            ColumnDescriptor columnDescriptor = tupleDescription.get(i);
            Object field = fields.get(i);
            serializer.startField();
            serializer.writeField(columnDescriptor.getDataType(), field);
            serializer.endField();
        }
        serializer.endRow();
    }

    /**
     * Returns a key for the QuerySession object. TransactionID is not
     * sufficient to key the cache. For the case where we have multiple
     * slices (i.e select a, b from c where a = 'part1' union all
     * select a, b from c where a = 'part2'), the query context will be
     * different for each slice, but the transactionID will be the same.
     * For that reason we must include the server name, data source and the
     * filter string as part of the QuerySession cache.
     *
     * @param context the request context
     * @return the key for the queue cache
     */
    private String getCacheKey(RequestContext context) {
        return String.format("%s:%s:%s:%s",
            context.getServerName(),
            context.getTransactionId(),
            context.getDataSource(),
            context.getFilterString());
    }

    /**
     * Query session holds state for the duration of the query, for all
     * segments for the same transaction, server name, data source and filter
     * string combination.
     *
     * @param cacheKey the key to the cache
     * @param context  the request context
     * @return the QuerySession object for the given key
     */
    private QuerySession<?, ?> getQuerySession(final String cacheKey, final RequestContext context) throws ExecutionException {
        return OUTPUT_QUEUE_CACHE.get(cacheKey, () -> {
            LOG.debug("Caching QuerySession for transactionId={} from segmentId={} with key={}",
                context.getTransactionId(), context.getSegmentId(), cacheKey);
            return new QuerySession<>(cacheKey, context.getTotalSegments());
        });
    }
}
