package org.greenplum.pxf.api.examples;

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Demo implementation of a processor
 */
@Component
public class DemoProcessor implements Processor<String> {

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession session) {
        return new DemoDataSplitter(session.getContext());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<String> getTupleIterator(QuerySession session, DataSplit split) {
        return new DemoTupleIterator(session.getContext(), (DemoFragmentMetadata) split.getMetadata());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Object> getFields(QuerySession session, String tuple) {
        return new TupleItr(tuple);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(QuerySession session) {
        RequestContext context = session.getContext();
        return StringUtils.isEmpty(context.getFormat()) &&
                StringUtils.equalsIgnoreCase("demo", context.getProtocol());
    }

    /**
     * An iterator that produces tuples for a given {@link RequestContext} and
     * {@link DemoFragmentMetadata}.
     */
    private static class DemoTupleIterator implements TupleIterator<String> {

        private int rowNumber;
        private final int numRows;
        private final int columnCount;
        private final String path;
        private final StringBuilder columnValue;

        DemoTupleIterator(RequestContext context, DemoFragmentMetadata metadata) {
            numRows = 2000000000;
            columnValue = new StringBuilder();
            columnCount = context.getColumns();
            path = metadata.getPath();
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
        public String next() {
            if (rowNumber >= numRows)
                throw new NoSuchElementException();

            columnValue.setLength(0);
            columnValue.append(path).append(" row").append(rowNumber + 1);
            for (int colIndex = 1; colIndex < columnCount; colIndex++) {
                columnValue.append("|").append("value").append(colIndex);
            }
            rowNumber++;
            return columnValue.toString();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() {
            // DO NOTHING
        }
    }

    /**
     * An iterator that splits the String tuple and returns an iterator over
     * the String splits
     */
    private static class TupleItr implements Iterator<Object> {
        private int currentField;
        private final String[] fields;

        public TupleItr(String tuple) {
            currentField = 0;
            fields = tuple.split("\\|");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return currentField < fields.length;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String next() {
            if (currentField >= fields.length)
                throw new NoSuchElementException();
            return fields[currentField++];
        }
    }
}
