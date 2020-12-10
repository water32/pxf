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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.greenplum.pxf.api.model.RequestContext.RequestType.SCAN_CONTROLLER;

/**
 * The {@code QuerySessionManager} manages {@code QuerySession} objects.
 * This includes the initialization and caching of {@code QuerySession}
 * objects.
 */
@Service
public class QuerySessionManager {

    private static final long EXPIRE_AFTER_ACCESS_DURATION_MILLIS = 5;

    private static final String DATA_SOURCE_HEADER = "X-GP-DATA-DIR";
    private static final String FILTER_HEADER = "X-GP-FILTER";
    private static final String SEGMENT_ID_HEADER = "X-GP-SEGMENT-ID";
    private static final String SERVER_HEADER = "X-GP-OPTIONS-SERVER";
    private static final String TRANSACTION_ID_HEADER = "X-GP-XID";

    private static final String MISSING_HEADER_ERROR = "Header %s is missing in the request";
    private static final String EMPTY_HEADER_ERROR = "Header %s is empty in the request";

    private final ApplicationContext applicationContext;
    private final Cache<String, QuerySession> querySessionCache;
    private final ConfigurationFactory configurationFactory;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final RequestParser<MultiValueMap<String, String>> parser;

    QuerySessionManager(ApplicationContext applicationContext,
                        ConfigurationFactory configurationFactory,
                        RequestParser<MultiValueMap<String, String>> parser) {
        this.querySessionCache = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_DURATION_MILLIS, TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, QuerySession>) notification ->
                        LOG.debug("Removed querySessionCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();

        this.applicationContext = applicationContext;
        this.configurationFactory = configurationFactory;
        this.parser = parser;
    }

    public QuerySession get(final MultiValueMap<String, String> headers)
            throws Throwable {

        final String dataSource = getHeaderValue(headers, DATA_SOURCE_HEADER, true);
        final String filterString = getHeaderValue(headers, FILTER_HEADER, false);
        final String serverName = getHeaderValue(headers, SERVER_HEADER, true);
        final int segmentId = getHeaderValueInt(headers, SEGMENT_ID_HEADER);
        final String transactionId = getHeaderValue(headers, TRANSACTION_ID_HEADER, true);

        final String cacheKey = String.format("%s:%s:%s:%s",
                serverName, transactionId, dataSource, filterString);

        try {
            QuerySession querySession = querySessionCache
                    .get(cacheKey, () -> {
                        LOG.debug("Caching querySession for key={} from segmentId={}",
                                cacheKey, segmentId);
                        QuerySession session = initializeQuerySession(headers);
                        initializeProducerTask(session);
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

    private QuerySession initializeQuerySession(MultiValueMap<String, String> headers) {

        // Parses the request once
        RequestContext context = parser.parseRequest(headers, SCAN_CONTROLLER);

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

    public Processor<?> getProcessor(RequestContext context) {
        return (Processor<?>) applicationContext
                .getBeansOfType(Processor.class)
                .values()
                .stream()
                .filter(p -> p.canProcessRequest(context))
                .findFirst()
                .orElseThrow(() -> {
                    String errorMessage;
                    if (StringUtils.isBlank(context.getFormat())) {
                        errorMessage = String.format("There are no registered processors to handle the '%s' protocol", context.getProtocol());
                    } else {
                        errorMessage = String.format("There are no registered processors to handle the '%s' protocol and '%s' format", context.getProtocol(), context.getFormat());
                    }
                    return new IllegalArgumentException(errorMessage);
                });
    }

    private void initializeProducerTask(QuerySession querySession) {
        // TODO: schedule work for the given query session.
        //  queryManager.register(querySession, segmentId);
        // Schedule splits


    }

    /**
     * Returns the first value from the request headers for the given
     * {@code headerKey}. When the header is required and the header is null
     * or empty, an {@link IllegalArgumentException} is thrown.
     *
     * @param headers   the request headers
     * @param headerKey the key for the header
     * @param required  whether the header is required or not
     * @return the header value
     * @throws IllegalArgumentException when the header is required and the value is null or empty
     */
    private String getHeaderValue(MultiValueMap<String, String> headers, String headerKey, boolean required)
            throws IllegalArgumentException {
        String value = headers.getFirst(headerKey);
        if (required && value == null) {
            throw new IllegalArgumentException(String.format(MISSING_HEADER_ERROR, headerKey));
        } else if (required && value.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format(EMPTY_HEADER_ERROR, headerKey));
        }
        return value;
    }

    /**
     * Returns the integer value for the given {@code headerKey}
     *
     * @param headers   the request headers
     * @param headerKey the key for the header
     * @return the integer header value
     * @throws IllegalArgumentException when the header is required and the value is null or empty
     */
    private int getHeaderValueInt(MultiValueMap<String, String> headers, String headerKey)
            throws IllegalArgumentException {
        String value = getHeaderValue(headers, headerKey, true);
        return Integer.parseInt(value);
    }
}
