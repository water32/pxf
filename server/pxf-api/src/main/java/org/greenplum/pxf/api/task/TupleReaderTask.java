package org.greenplum.pxf.api.task;

import org.apache.catalina.connector.ClientAbortException;
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
public class TupleReaderTask<T> implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 10240;

    /**
     * Name of the property that allows overriding the default batch size
     */
    private static final String PXF_TUPLE_READER_BATCH_SIZE_PROPERTY = "pxf.processor.batch-size";

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);

    private final int taskNumber;
    private final DataSplit split;
    private final BlockingQueue<List<T>> outputQueue;
    private final QuerySession<T> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;
    private Thread runningThread;

    private volatile boolean isCancelled;

    public TupleReaderTask(int taskNumber, DataSplit split, QuerySession<T> querySession) {
        this.taskNumber = taskNumber;
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.processor = querySession.getProcessor();
        this.uniqueResourceName = split.toString();
        this.isCancelled = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        this.runningThread = Thread.currentThread();
        if (isCancelled)
            return; // Query is no longer active because of an error or cancellation

        int totalRows = 0;
        int batchSize = querySession.getContext().getConfiguration()
                .getInt(PXF_TUPLE_READER_BATCH_SIZE_PROPERTY, DEFAULT_BATCH_SIZE);
        TupleIterator<T> iterator = null;
        try {
            iterator = processor.getTupleIterator(querySession, split);
            List<T> batch = new ArrayList<>(batchSize);
            while (!isCancelled && iterator.hasNext()) {
                batch.add(iterator.next());
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (!isCancelled && !batch.isEmpty()) {
                totalRows += batch.size();
                outputQueue.put(batch);
            }
        } catch (InterruptedException e) {
            LOG.debug("TupleReaderTask with thread ID " + runningThread.getId() + " has been interrupted");
        } catch (ClientAbortException e) {
            querySession.cancelQuery(e);
        } catch (IOException e) {
            querySession.errorQuery(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (Exception e) {
            querySession.errorQuery(e);
        } finally {

            // Clear the interrupted status of this thread by calling Thread.interrupted()
            Thread.interrupted();

            querySession.removeTupleReaderTask(taskNumber, this);
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

    /**
     * Cancel this {@link TupleReaderTask}
     */
    public void cancel() {
        isCancelled = true;
        if (runningThread != null) {
            runningThread.interrupt();
        }
    }
}

