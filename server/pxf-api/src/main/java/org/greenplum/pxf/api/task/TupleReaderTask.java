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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;

/**
 * Processes a {@link DataSplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class TupleReaderTask<T> implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 1024;
    // TODO: decide a property name for the batch size
    private static final String PXF_TUPLE_READER_BATCH_SIZE_PROPERTY = "pxf.tuple-reader.batch-size";

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);

    private final DataSplit split;
    private final BlockingDeque<List<List<Object>>> outputQueue;
    private final QuerySession querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;
    private Thread runningThread;

    @SuppressWarnings("unchecked")
    public TupleReaderTask(DataSplit split, QuerySession querySession) {
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.processor = (Processor<T>) querySession.getProcessor();
        this.uniqueResourceName = split.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        if (!querySession.isActive())
            return; // Query is no longer active because of an error or cancellation

        this.runningThread = Thread.currentThread();

        int totalRows = 0;
        int batchSize = querySession.getContext().getConfiguration()
                .getInt(PXF_TUPLE_READER_BATCH_SIZE_PROPERTY, DEFAULT_BATCH_SIZE);
        TupleIterator<T> iterator = null;
        try {
            iterator = processor.getTupleIterator(querySession, split);
            List<List<Object>> batch = new ArrayList<>(batchSize);
            while (querySession.isActive() && iterator.hasNext()) {
                List<Object> fields = materializeIterator(processor.getFields(querySession, iterator.next()));
                batch.add(fields);
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(batch);
                    // TODO: when the outputQueue is full we might want to sleep this
                    //       thread and reschedule it until later (backpressure)
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (!batch.isEmpty() && querySession.isActive()) {
                totalRows += batch.size();
                outputQueue.put(batch);
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery(e);
        } catch (IOException e) {
            querySession.errorQuery(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (Exception e) {
            querySession.errorQuery(e);
        } finally {
            querySession.removeTupleReaderTask(this);
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
     * Fully consumes the iterator and materializes it into a list of objects
     *
     * @param iterator the field iterator
     * @return the list of iterator
     */
    private List<Object> materializeIterator(Iterator<Object> iterator) {
        List<Object> list = new ArrayList<>(querySession.getContext().getTupleDescription().size());
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    /**
     * Interrupts this {@link TupleReaderTask}
     */
    public void interrupt() {
        if (runningThread != null) {
            runningThread.interrupt();
        }
    }
}

