package org.greenplum.pxf.api.examples;

import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoDataSplitterTest {

    @Test
    void testIterator() {
        RequestContext context = new RequestContext();
        context.setDataSource("demo-datasource");

        DemoDataSplitter splitter = new DemoDataSplitter(context);

        for (int i=1; i <= DemoDataSplitter.TOTAL_FRAGMENTS; i++) {
            assertTrue(splitter.hasNext());
            assertTrue(splitter.hasNext()); // should not skip any splits
            DataSplit split = splitter.next();
            assertNotNull(split);
            assertEquals(split.getResource(), "demo-datasource." + i);
        }

        assertFalse(splitter.hasNext());
        assertThrows(NoSuchElementException.class, splitter::next);
    }
}