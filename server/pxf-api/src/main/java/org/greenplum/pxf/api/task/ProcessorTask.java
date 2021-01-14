package org.greenplum.pxf.api.task;

import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.TupleBatch;
import org.greenplum.pxf.api.model.TupleIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Processes a {@link DataSplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class ProcessorTask<T, M> implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 10240;

    /**
     * Name of the property that allows overriding the default batch size
     */
    private static final String PXF_TUPLE_READER_BATCH_SIZE_PROPERTY = "pxf.processor.batch-size";

    private final Logger LOG = LoggerFactory.getLogger(ProcessorTask.class);

    private final DataSplit split;
    private final BlockingQueue<TupleBatch<T, M>> outputQueue;
    private final QuerySession<T, M> querySession;
    private final String uniqueResourceName;
    private final Processor<T, M> processor;

    public ProcessorTask(DataSplit split, QuerySession<T, M> querySession) {
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.processor = querySession.getProcessor();
        this.uniqueResourceName = split.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        int totalRows = 0;
        int batchSize = querySession.getContext().getConfiguration()
                .getInt(PXF_TUPLE_READER_BATCH_SIZE_PROPERTY, DEFAULT_BATCH_SIZE);
        TupleIterator<T, M> iterator = null;
        Thread currentThread = Thread.currentThread();
        try {
            iterator = processor.getTupleIterator(querySession, split);
            TupleBatch<T, M> tupleBatch = new TupleBatch<>(batchSize, iterator.getMetadata());
            while (!currentThread.isInterrupted() && iterator.hasNext()) {
                tupleBatch.add(iterator.next());

                if (tupleBatch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(tupleBatch);
                    tupleBatch = new TupleBatch<>(batchSize, iterator.getMetadata());
                    Thread.sleep(200);
                }
            }
            if (!currentThread.isInterrupted() && !tupleBatch.isEmpty()) {
                totalRows += tupleBatch.size();
                outputQueue.put(tupleBatch);
            }
        } catch (InterruptedException e) {
            LOG.debug("ProcessorTask with thread ID {} has been interrupted", currentThread.getId());

            // Reset the interrupt flag
            currentThread.interrupt();
        } catch (Exception e) {
            querySession.errorQuery(e, false);
            LOG.error(String.format("Error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } finally {

            querySession.markTaskAsCompleted();
            if (iterator != null) {
                try {
                    iterator.cleanup();
                } catch (Exception e) {
                    // ignore ... any significant errors should already have been handled
                }
            }
        }

        // Keep track of the number of records processed by this task
        LOG.debug("completed processing {} row{} {} for query {}",
                totalRows, totalRows == 1 ? "" : "s", uniqueResourceName, querySession);
    }
}

