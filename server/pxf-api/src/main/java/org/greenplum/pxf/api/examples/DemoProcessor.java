package org.greenplum.pxf.api.examples;

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.api.serializer.TupleSerializer;
import org.springframework.stereotype.Component;

import java.util.NoSuchElementException;

/**
 * Demo implementation of a processor
 */
@Component
public class DemoProcessor implements Processor<String[], Void> {

    static final int TOTAL_ROWS = 200_000;

    private final DemoTupleSerializer tupleSerializer;

    public DemoProcessor(DemoTupleSerializer tupleSerializer) {
        this.tupleSerializer = tupleSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession<String[], Void> session) {
        return new DemoDataSplitter(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<String[], Void> getTupleIterator(QuerySession<String[], Void> session, DataSplit split) {
        return new DemoTupleIterator(session.getContext(), (DemoFragmentMetadata) split.getMetadata());
    }

    @Override
    public TupleSerializer<String[], Void> tupleSerializer(QuerySession<String[], Void> querySession) {
        return tupleSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(QuerySession<String[], Void> session) {
        RequestContext context = session.getContext();
        return StringUtils.isEmpty(context.getFormat()) &&
                StringUtils.equalsIgnoreCase("demo", context.getProtocol());
    }

    private static String stringMapper(QuerySession<String[], Void> session, String[] tuple, int columnIndex) {
        return tuple[columnIndex];
    }

    /**
     * An iterator that produces tuples for a given {@link RequestContext} and
     * {@link DemoFragmentMetadata}.
     */
    private static class DemoTupleIterator implements TupleIterator<String[], Void> {

        private final int numRows = TOTAL_ROWS;

        private final int columnCount;
        private final String path;
        private final String[] tuple;

        private int rowNumber;

        DemoTupleIterator(RequestContext context, DemoFragmentMetadata metadata) {
            columnCount = context.getColumns();
            path = metadata.getPath();
            tuple = new String[columnCount];
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return rowNumber < numRows;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String[] next() {
            if (rowNumber >= numRows)
                throw new NoSuchElementException();

            tuple[0] = path + " row" + (rowNumber + 1);

            for (int colIndex = 1; colIndex < columnCount; colIndex++) {
                tuple[colIndex] = "value" + colIndex;
            }
            rowNumber++;
            return tuple;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Void getMetadata() {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() { // DO NOTHING
        }
    }
}
