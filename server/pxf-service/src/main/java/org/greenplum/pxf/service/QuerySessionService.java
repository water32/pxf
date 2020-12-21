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

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.greenplum.pxf.service.PxfConfiguration.PXF_PRODUCER_TASK_EXECUTOR;
import static org.greenplum.pxf.service.PxfConfiguration.PXF_TUPLE_TASK_EXECUTOR;

/**
 * The {@code QuerySessionManager} manages {@link QuerySession} objects.
 * This includes the initialization and caching of {@link QuerySession}
 * objects. It is also in charge of scheduling {@link ProducerTask} jobs
 * for newly created {@link QuerySession} objects.
 */
@Service
public class QuerySessionService<T> {

    private static final long EXPIRE_AFTER_ACCESS_DURATION_MINUTES = 5;

    private final Cache<String, QuerySession<T>> querySessionCache;
    private final ConfigurationFactory configurationFactory;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

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
     */
    QuerySessionService(ApplicationContext applicationContext,
                        ConfigurationFactory configurationFactory,
                        ListableBeanFactory beanFactory) {
        this.querySessionCache = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_DURATION_MINUTES, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, QuerySession<T>>) notification ->
                        LOG.debug("Removed querySessionCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();

        this.configurationFactory = configurationFactory;
        this.producerTaskExecutor = (Executor) beanFactory.getBean(PXF_PRODUCER_TASK_EXECUTOR);
        this.tupleTaskExecutor = (Executor) beanFactory.getBean(PXF_TUPLE_TASK_EXECUTOR);
        this.registeredProcessors = applicationContext.getBeansOfType(Processor.class).values();
    }

    /**
     * Returns the {@link QuerySession} for the given request headers
     *
     * @param context the request context
     * @return the {@link QuerySession} for the given request headers
     * @throws Throwable when an error occurs
     */
    public QuerySession<T> get(final RequestContext context)
            throws Throwable {

        try {
            // Lookup the querySession from the cache, if the querySession
            // is not in the cache, it will be initialized and a
            // ProducerTask will be scheduled for the new querySession.
            QuerySession<T> querySession = getQuerySessionFromCache(context);
            try {
                // Register the segment to the session
                querySession.registerSegment(context.getSegmentId());
            } catch (IllegalStateException ex) {
                LOG.debug("QuerySession {} is no longer active", querySession);

                // Retrieve a new querySession from cache, the new querySession
                // should not be expired
                if (!querySession.isQueryCancelled() && !querySession.isQueryErrored()) {
                    querySession = getQuerySessionFromCache(context);
                    querySession.registerSegment(context.getSegmentId());
                } else {
                    return null;
                }
            }

            return querySession;
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            if (e.getCause() != null)
                throw e.getCause();
            throw e;
        }
    }

    /**
     * Returns the {@link QuerySession} for the given {@link RequestContext}
     *
     * @param context the context for the request
     * @return the QuerySession for the current request
     * @throws ExecutionException when an execution error occurs when retrieving the cache entry
     */
    private QuerySession<T> getQuerySessionFromCache(final RequestContext context)
            throws ExecutionException {

        final String cacheKey = String.format("%s:%s:%s:%s",
                context.getServerName(), context.getTransactionId(),
                context.getDataSource(), context.getFilterString());

        return querySessionCache
                .get(cacheKey, () -> {
                    LOG.debug("Caching querySession for key={} from segmentId={}",
                            cacheKey, context.getSegmentId());
                    QuerySession<T> session = initializeQuerySession(context);
                    initializeAndExecuteProducerTask(session);
                    return session;
                });
    }

    /**
     * Parses the request headers and initializes the {@link QuerySession} with
     * the context and the processor for the request.
     *
     * @param context the request context
     * @return the initialized querySession
     */
    private QuerySession<T> initializeQuerySession(RequestContext context) {

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

        QuerySession<T> session = new QuerySession<>(context, querySessionCache);
        session.setProcessor(getProcessor(session));

        LOG.info("Initialized querySession {}", session);

        return session;
    }

    /**
     * Creates a {@link ProducerTask} and executes the task in the
     * {@code producerTaskExecutor} for the provided {@code querySession}.
     *
     * @param querySession the query session to use for the producer
     */
    private void initializeAndExecuteProducerTask(QuerySession<T> querySession) {
        // Execute the ProducerTask
        ProducerTask<T> producer = new ProducerTask<>(querySession, tupleTaskExecutor);
        producerTaskExecutor.execute(producer);
    }

    /**
     * Returns the processor that can handle the request
     *
     * @param querySession the session for the query
     * @return the processor that can handle the request
     */
    @SuppressWarnings("unchecked")
    public Processor<T> getProcessor(QuerySession<T> querySession) {
        return (Processor<T>) registeredProcessors
                .stream()
                .filter(p -> p.canProcessRequest(querySession))
                .findFirst()
                .orElseThrow(() -> {
                    RequestContext context = querySession.getContext();
                    String errorMessage = String.format("There are no registered processors to handle the '%s' protocol", context.getProtocol());
                    if (StringUtils.isNotBlank(context.getFormat())) {
                        errorMessage += String.format(" and '%s' format", context.getFormat());
                    }
                    return new IllegalArgumentException(errorMessage);
                });
    }
}
