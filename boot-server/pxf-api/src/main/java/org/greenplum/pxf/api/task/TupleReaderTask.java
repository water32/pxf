package org.greenplum.pxf.api.task;

import com.google.common.collect.Lists;
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
import java.util.concurrent.BlockingDeque;

/**
 * Processes a {@link DataSplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class TupleReaderTask<T, M> implements Runnable {

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);
    private final DataSplit split;
    private final BlockingDeque<List<List<Object>>> outputQueue;
    private final QuerySession<T, M> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;

    public TupleReaderTask(Processor<T> processor, DataSplit split, QuerySession<T, M> querySession) {
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.uniqueResourceName = split.toString();
        this.processor = processor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        if (!querySession.isActive()) {
            // Query is no longer active because of an error or cancellation
            return;
        }

        // TODO: control the batch size through query param to see if we get better throughput
        int batchSize = 250, totalRows = 0;
        TupleIterator<T> iterator = null;
        try {
            iterator = processor.getTupleIterator(split);
            List<List<Object>> batch = new ArrayList<>(batchSize);
            while (querySession.isActive() && iterator.hasNext()) {
                T tuple = iterator.next();
                List<Object> fields = Lists.newArrayList(processor.getFields(tuple));
                batch.add(fields);
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(batch);
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
            if (iterator != null) {
                try {
                    iterator.cleanup();
                } catch (Exception e) {
                    // ignore ... any significant errors should already have been handled
                }
            }
            querySession.registerCompletedTask();
        }

        // Keep track of the number of records processed by this task
        LOG.debug("completed processing {} row{} {} for query {}",
            totalRows, totalRows == 1 ? "" : "s", uniqueResourceName, querySession);
    }

    public int getOutputQueueSize() {
        return outputQueue.size();
    }
}
