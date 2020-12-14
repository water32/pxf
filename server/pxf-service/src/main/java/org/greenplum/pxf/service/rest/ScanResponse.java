package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.serializer.BinarySerializer;
import org.greenplum.pxf.api.serializer.CsvSerializer;
import org.greenplum.pxf.api.serializer.Serializer;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class ScanResponse implements StreamingResponseBody {

    private static final Logger LOG = LoggerFactory.getLogger(ScanResponse.class);

    private final QuerySession querySession;
    private final RequestContext context;
    private final List<ColumnDescriptor> columnDescriptors;

    public ScanResponse(QuerySession querySession) {
        this.querySession = querySession;
        this.context = querySession.getContext();
        this.columnDescriptors = context.getTupleDescription();
    }

    @SneakyThrows
    @Override
    public void writeTo(OutputStream output) {

        LOG.debug("{}-{}-- Starting streaming for {}",
                context.getTransactionId(),
                context.getSegmentId(),
                querySession);

        int recordCount = 0;
        BlockingDeque<List<List<Object>>> outputQueue = querySession.getOutputQueue();
        List<ColumnDescriptor> columnDescriptors = this.columnDescriptors;
        try {
            Serializer serializer = getSerializer();
            serializer.open(output);

            while (querySession.isActive()) {
                List<List<Object>> batch = outputQueue.poll(10, TimeUnit.MILLISECONDS);
                if (batch != null) {
                    for (List<Object> tuple : batch) {
                        serializer.startRow(columnDescriptors.size());
                        for (int i = 0; i < columnDescriptors.size(); i++) {
                            ColumnDescriptor columnDescriptor = columnDescriptors.get(i);
                            Object field = tuple.get(i);
                            serializer.startField();
                            serializer.writeField(columnDescriptor.getDataType(), field);
                            serializer.endField();
                        }
                        serializer.endRow();
                    }
                    recordCount += batch.size();
                } else if (querySession.hasFinishedProducing()
                        && (querySession.getCompletedTaskCount() == querySession.getCreatedTaskCount())
                        && outputQueue.isEmpty()) {
                    break;
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
                LOG.warn(String.format("Remote connection closed by Greenplum (segment %s)", context.getSegmentId()), e);
            } else {
                LOG.warn("Remote connection closed by Greenplum (segment {}) (Enable debug for stacktrace)", context.getSegmentId());
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            querySession.errorQuery(e);
            LOG.error(e.getMessage() != null ? e.getMessage() : "ERROR", e);
            throw new IOException(e.getMessage(), e);
        } finally {
            querySession.deregisterSegment(recordCount);
            LOG.debug("{}-{}-- Stopped streaming {} record{} for {}",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    recordCount,
                    recordCount == 1 ? "" : "s",
                    querySession);
        }
    }

    /**
     * Returns the on-the-wire serializer
     *
     * @return the serializer
     */
    public Serializer getSerializer() {
        switch (context.getOutputFormat()) {
            case TEXT:
                return new CsvSerializer(context.getGreenplumCSV());
            case Binary:
                return new BinarySerializer();
            default:
                throw new UnsupportedOperationException(
                        String.format("The output format '%s' is not supported", context.getOutputFormat()));
        }
    }
}
