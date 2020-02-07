package org.greenplum.pxf.api.examples;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.RequestContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Internal interface that would defined the access to a file on HDFS, but in
 * this case contains the data required.
 * <p>
 * Demo implementation
 */
@Component
@Scope("prototype")
public class DemoProcessor extends BaseProcessor<String, Void> {

    @Value("${org.greenplum.pxf.api.examples.DemoProcessor.numRows:2}")
    private int numRows = 3;

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<String> getTupleIterator(DataSplit split) {
        final String fragmentMetadata = new String(split.getMetadata());
        final int colCount = context.getColumns();

        return new Iterator<String>() {
            private int rowNumber;
            private StringBuilder colValue = new StringBuilder();

            @Override
            public boolean hasNext() {
                return rowNumber < numRows;
            }

            @Override
            public String next() {
                if (rowNumber >= numRows)
                    throw new NoSuchElementException();

                colValue.setLength(0);
                colValue.append(fragmentMetadata).append(" row").append(rowNumber + 1);
                for (int colIndex = 1; colIndex < colCount; colIndex++) {
                    colValue.append("|").append("value").append(colIndex);
                }
                rowNumber++;
                return colValue.toString();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Object> getFields(String tuple) {
        return new TupleItr(tuple);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DemoDataSplitter getQuerySplitter() {
        return new DemoDataSplitter(context, configuration);
    }

    /**
     * Process requests with "demo" protocol and no format
     *
     * @param context the context for the request
     * @return true if the Processor can process the request, false otherwise
     */
    @Override
    public boolean canProcessRequest(RequestContext context) {
        return StringUtils.isEmpty(context.getFormat()) &&
            StringUtils.equalsIgnoreCase("demo", context.getProtocol());
    }

    /**
     * An iterator that splits the tuple and returns an iterator over the
     * splits
     */
    private static class TupleItr implements Iterator<Object> {
        private int currentField;
        private String[] fields;

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
        public Object next() {
            if (currentField >= fields.length)
                throw new NoSuchElementException();
            return fields[currentField++];
        }
    }
}
