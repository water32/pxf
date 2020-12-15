package org.greenplum.pxf.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.ConfigurationFactory;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.task.ProducerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.greenplum.pxf.api.model.RequestContext.RequestType.SCAN_CONTROLLER;
import static org.greenplum.pxf.service.PxfConfiguration.PXF_PRODUCER_TASK_EXECUTOR;
import static org.greenplum.pxf.service.PxfConfiguration.PXF_TUPLE_TASK_EXECUTOR;

/**
 * The {@code QuerySessionManager} manages {@link QuerySession} objects.
 * This includes the initialization and caching of {@link QuerySession}
 * objects. It is also in charge of scheduling {@link ProducerTask} jobs
 * for newly created {@link QuerySession} objects.
 */
@Service
public class QuerySessionManager {

    private static final long EXPIRE_AFTER_ACCESS_DURATION_MILLIS = 5;

    private final Cache<String, QuerySession> querySessionCache;
    private final ConfigurationFactory configurationFactory;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final RequestParser<MultiValueMap<String, String>> parser;
    private final Executor producerTaskExecutor;
    private final Executor tupleTaskExecutor;
    @SuppressWarnings("rawtypes")
    private final Collection<Processor> registeredProcessors;

    /**
     * Initializes a QuerySessionManager with auto-wired components
     *
     * @param applicationContext   the application context
     * @param configurationFactory the configuration factory
     * @param beanFactory          the bean factory
     * @param parser               the request parser
     */
    QuerySessionManager(ApplicationContext applicationContext,
                        ConfigurationFactory configurationFactory,
                        ListableBeanFactory beanFactory,
                        RequestParser<MultiValueMap<String, String>> parser) {
        this.querySessionCache = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_DURATION_MILLIS, TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, QuerySession>) notification ->
                        LOG.debug("Removed querySessionCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();

        this.configurationFactory = configurationFactory;
        this.parser = parser;
        this.producerTaskExecutor = (Executor) beanFactory.getBean(PXF_PRODUCER_TASK_EXECUTOR);
        this.tupleTaskExecutor = (Executor) beanFactory.getBean(PXF_TUPLE_TASK_EXECUTOR);
        this.registeredProcessors = applicationContext.getBeansOfType(Processor.class).values();
    }

    /**
     * Returns the {@link QuerySession} for the given request headers
     *
     * @param headers the request headers
     * @return the {@link QuerySession} for the given request headers
     * @throws Throwable when an error occurs
     */
    public QuerySession get(final MultiValueMap<String, String> headers)
            throws Throwable {

        // TODO: should we do minimal parsing? we only need server name, xid,
        //       resource, filter string and segment ID. Minimal parsing can
        //       improve the query performance marginally
        final RequestContext context = parser.parseRequest(headers, SCAN_CONTROLLER);
        final String cacheKey = String.format("%s:%s:%s:%s",
                context.getServerName(), context.getTransactionId(),
                context.getDataSource(), context.getFilterString());
        final int segmentId = context.getSegmentId();

        try {
            // Lookup the querySession from the cache, if the querySession
            // is not in the cache, it will be initialized and a
            // ProducerTask will be scheduled for the new querySession.
            QuerySession querySession = querySessionCache
                    .get(cacheKey, () -> {
                        LOG.debug("Caching querySession for key={} from segmentId={}",
                                cacheKey, segmentId);
                        QuerySession session = initializeQuerySession(context);
                        initializeAndExecuteProducerTask(session);
                        return session;
                    });

            // Register the segment to the session
            querySession.registerSegment(segmentId);

            return querySession;
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            if (e.getCause() != null)
                throw e.getCause();
            throw e;
        }
    }

    /**
     * Parses the request headers and initializes the {@link QuerySession} with
     * the context and the processor for the request.
     *
     * @param context the request headers
     * @return the initialized querySession
     */
    private QuerySession initializeQuerySession(RequestContext context) {

        // Initialize the configuration for this request
        // Configuration initialization is expensive, so we only
        // want to do it once per query session
        Configuration configuration = configurationFactory.
                initConfiguration(
                        context.getConfig(),
                        context.getServerName(),
                        context.getUser(),
                        context.getAdditionalConfigProps());

        context.setConfiguration(configuration);

        return new QuerySession(context, getProcessor(context));
    }

    /**
     * Creates a {@link ProducerTask} and executes the task in the
     * {@code producerTaskExecutor} for the provided {@code querySession}.
     *
     * @param querySession the query session to use for the producer
     */
    private void initializeAndExecuteProducerTask(QuerySession querySession) {
        // Execute the ProducerTask
        ProducerTask producer = new ProducerTask(querySession, tupleTaskExecutor);
        producerTaskExecutor.execute(producer);
    }

    /**
     * Returns the processor that can handle the request
     *
     * @param context the request context
     * @return the processor that can handle the request
     */
    public Processor<?> getProcessor(RequestContext context) {
        return (Processor<?>) registeredProcessors
                .stream()
                .filter(p -> p.canProcessRequest(context))
                .findFirst()
                .orElseThrow(() -> {
                    String errorMessage = String.format("There are no registered processors to handle the '%s' protocol", context.getProtocol());
                    if (StringUtils.isNotBlank(context.getFormat())) {
                        errorMessage += String.format(" and '%s' format", context.getFormat());
                    }
                    return new IllegalArgumentException(errorMessage);
                });
    }
}
