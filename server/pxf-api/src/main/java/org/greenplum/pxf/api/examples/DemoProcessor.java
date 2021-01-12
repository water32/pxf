package org.greenplum.pxf.api.examples;

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.function.TriFunction;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Demo implementation of a processor
 */
@Component
public class DemoProcessor implements Processor<String[]> {

    static final int TOTAL_ROWS = 200_000;

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession<String[]> session) {
        return new DemoDataSplitter(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<String[]> getTupleIterator(QuerySession<String[]> session, DataSplit split) {
        return new DemoTupleIterator(session.getContext(), (DemoFragmentMetadata) split.getMetadata());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(QuerySession<String[]> session) {
        RequestContext context = session.getContext();
        return StringUtils.isEmpty(context.getFormat()) &&
                StringUtils.equalsIgnoreCase("demo", context.getProtocol());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public TriFunction<QuerySession<String[]>, String[], Integer, Object>[] getMappingFunctions(QuerySession<String[]> session) {
        TriFunction<QuerySession<String[]>, String[], Integer, Object>[] functions;
        functions = new TriFunction[session.getContext().getColumns()];
        Arrays.fill(functions, (TriFunction<QuerySession<String[]>, String[], Integer, Object>) DemoProcessor::stringMapper);
        return functions;
    }

    private static String stringMapper(QuerySession<String[]> session, String[] s, int columnIndex) {
        return s[columnIndex];
    }

    /**
     * An iterator that produces tuples for a given {@link RequestContext} and
     * {@link DemoFragmentMetadata}.
     */
    private static class DemoTupleIterator implements TupleIterator<String[]> {

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
        public void cleanup() { // DO NOTHING
        }
    }
}
