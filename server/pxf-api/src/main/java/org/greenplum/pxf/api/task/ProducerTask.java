package org.greenplum.pxf.api.task;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitSegmentIterator;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.TupleBatch;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.greenplum.pxf.api.configuration.PxfServerProperties.DEFAULT_SCALE_FACTOR;
import static org.greenplum.pxf.api.factory.ConfigurationFactory.PXF_PROCESSOR_SCALE_FACTOR_PROPERTY;

public class ProducerTask<T, M> implements Runnable {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final QuerySession<T, M> querySession;
    private final ThreadPoolExecutor processorExecutorService;
    private int currentScaleFactor = 1;
    private final int maximumScaleFactor;

    public ProducerTask(QuerySession<T, M> querySession) {
        this.querySession = requireNonNull(querySession, "querySession cannot be null");
        // maxProcessorThreads defaults to MAX_PROCESSOR_THREADS_PER_SESSION
        // it can be overridden by setting the PXF_MAX_PROCESSOR_THREADS_PROPERTY
        // in the server configuration
        String poolId = querySession.getContext().getUser() + ":" + querySession.getContext().getTransactionId();

        maximumScaleFactor = querySession.getContext().getConfiguration()
                .getInt(PXF_PROCESSOR_SCALE_FACTOR_PROPERTY, DEFAULT_SCALE_FACTOR);
        int maxProcessorThreads = Utilities.getProcessorMaxThreadsPerSession(1);
        // We create a new ExecutorService with a fixed amount of threads.
        // We create one ExecutorService per query so we don't need to build
        // a customized ExecutorService
        this.processorExecutorService =
                createProcessorTaskExecutorService(poolId, 3);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        int totalSegments = querySession.getContext().getTotalSegments();
        Integer segmentId;
        Set<Integer> segmentIds;

        try {
            // Materialize the list of splits
            DataSplitter splitter = querySession.getProcessor().getDataSplitter(querySession);
            List<DataSplit> splitList = Lists.newArrayList(splitter);
            // get the queue of segments IDs that have registered to this QuerySession
            BlockingDeque<Integer> registeredSegmentQueue = querySession.getRegisteredSegmentQueue();
            BlockingQueue<TupleBatch<T, M>> outputQueue = querySession.getOutputQueue();

            while (querySession.isActive()) {
                // In case this thread is interrupted, the poll call below will
                // cause an InterruptedException, which will cause the
                // exception be caught, and the finally block will be
                // in charge of shutting down the ThreadPoolExecutor.
                segmentId = registeredSegmentQueue.poll(20, TimeUnit.MILLISECONDS);

                if (segmentId == null) {
                    int completed = querySession.getCompletedProcessorCount();
                    int created = querySession.getCreatedProcessorCount();
                    if (completed == created) {
                        // try to mark the session as inactive. If another
                        // thread is able to register itself before we mark it
                        // as inactive, or the output queue has elements
                        // still remaining to process, this operation will be
                        // a no-op
                        querySession.tryMarkInactive();
                    }

//                    int outputQueueSize = outputQueue.size();
//                    int executorQueueSize = processorExecutorService.getQueue().size();
//
//                    // We increase the executor pool size if all of these
//                    // conditions are met:
//                    // - We are consuming tuples from the output queue
//                    //   fast enough to keep the output queue almost empty.
//                    // - There is more work left to increase the
//                    //   number of threads.
//                    // - We have not reached the maximum scale factor.
//
//                    if (outputQueueSize <= 5
//                            && executorQueueSize > 0
//                            && currentScaleFactor < maximumScaleFactor) {
//                        currentScaleFactor++;
//                        // calculate the number of threads for the next scale factor
//                        int threads = Utilities.getProcessorMaxThreadsPerSession(currentScaleFactor);
//
//                        LOG.debug("{}: New scale factor is {}, output queue size is {} with {} tasks queued to be processed",
//                                querySession, currentScaleFactor, outputQueueSize, executorQueueSize);
//
//                        processorExecutorService.setMaximumPoolSize(threads);
//                        processorExecutorService.setCorePoolSize(threads);
//                    }
                } else {
                    // Add all the segment ids that are available
                    segmentIds = new HashSet<>();
                    segmentIds.add(segmentId);
                    while ((segmentId = registeredSegmentQueue.poll(5, TimeUnit.MILLISECONDS)) != null) {
                        segmentIds.add(segmentId);
                    }

                    LOG.debug("{}: ProducerTask with a set of {} segmentId(s)", querySession, segmentIds.size());

                    int createdTasks = 0;
                    Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(segmentIds, totalSegments, splitList);
                    while (iterator.hasNext() && querySession.isActive()) {
                        DataSplit split = iterator.next();
                        LOG.debug("{}: Submitting '{}' to the pool", querySession, split);

                        ProcessorTask<T, M> task = new ProcessorTask<>(split, querySession);

                        querySession.markTaskAsCreated();
                        processorExecutorService.execute(task);
                        createdTasks++;
                    }

                    LOG.debug("{}: Stopped producing {} tasks", querySession, createdTasks);
                }
            }
        } catch (InterruptedException ex) {
            querySession.cancelQuery();
            LOG.warn("{}: ProducerTask has been interrupted", querySession);
        } catch (Exception ex) {
            querySession.errorQuery(ex, false);
            throw new RuntimeException(ex);
        } finally {

            // At this point the query session is inactive for one of the
            // following reasons:
            //   1. There is no more work or tuples available to be processed
            //   2. The query was cancelled
            //   3. The query errored out

            if (querySession.isQueryErrored() || querySession.isQueryCancelled()) {
                // When an error occurs or the query is cancelled, we need
                // to cancel all the running ProcessorTasks by interrupting
                // them.
                processorExecutorService.shutdownNow();
            } else {
                processorExecutorService.shutdown();
            }

            try {
                if (!processorExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("{}: Pool did not shutdown within 10 seconds", querySession);
                }
            } catch (InterruptedException ignored) {
            }

            try {
                // We need to allow all of the ScanResponses to fully consume
                // the batches from the outputQueue
                // Wait with timeout in case we miss the signal. If we
                // miss the signal we don't want to wait forever.
                querySession.waitForAllSegmentsToDeregister();
            } catch (InterruptedException ignored) {
            }

            try {
                querySession.close();
            } catch (Exception ex) {
                LOG.warn(String.format("Error while closing the QuerySession %s", querySession), ex);
            }
        }
    }

    private ThreadPoolExecutor createProcessorTaskExecutorService(String poolId, int nThreads) {
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("pxf-processor-" + poolId + "-%d")
                        .build();
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                namedThreadFactory);
    }
}
