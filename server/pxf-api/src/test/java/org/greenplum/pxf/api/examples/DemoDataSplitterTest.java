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

        assertTrue(splitter.hasNext());
        DataSplit split = splitter.next();
        assertNotNull(split);
        assertEquals(split.getResource(), "demo-datasource.1");

        assertTrue(splitter.hasNext());
        assertTrue(splitter.hasNext()); // should not skip any splits
        split = splitter.next();
        assertNotNull(split);
        assertEquals(split.getResource(), "demo-datasource.2");

        assertTrue(splitter.hasNext());
        split = splitter.next();
        assertNotNull(split);
        assertEquals(split.getResource(), "demo-datasource.3");

        assertFalse(splitter.hasNext());
        assertThrows(NoSuchElementException.class, splitter::next);
    }
}