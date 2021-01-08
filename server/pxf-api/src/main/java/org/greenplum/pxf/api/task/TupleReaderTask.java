package org.greenplum.pxf.api.task;

import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.TupleIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Processes a {@link DataSplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
// TODO: rename to ProcessorTask
public class TupleReaderTask<T> implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 10240;

    /**
     * Name of the property that allows overriding the default batch size
     */
    private static final String PXF_TUPLE_READER_BATCH_SIZE_PROPERTY = "pxf.processor.batch-size";

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);

    private final DataSplit split;
    private final BlockingQueue<List<T>> outputQueue;
    private final QuerySession<T> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;

    public TupleReaderTask(DataSplit split, QuerySession<T> querySession) {
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
        TupleIterator<T> iterator = null;
        Thread currentThread = Thread.currentThread();
        try {
            iterator = processor.getTupleIterator(querySession, split);
            List<T> batch = new ArrayList<>(batchSize);
            while (!currentThread.isInterrupted() && iterator.hasNext()) {
                batch.add(iterator.next());
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (!currentThread.isInterrupted() && !batch.isEmpty()) {
                totalRows += batch.size();
                outputQueue.put(batch);
            }
        } catch (InterruptedException e) {
            LOG.debug("TupleReaderTask with thread ID {} has been interrupted", currentThread.getId());

            // Reset the interrupt flag
            currentThread.interrupt();
        } catch (IOException e) {
            querySession.errorQuery(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (Exception e) {
            querySession.errorQuery(e);
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

