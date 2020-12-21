package org.greenplum.pxf.api.examples;

import com.google.common.cache.Cache;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DemoProcessorTest {

    private Processor<String[]> processor;
    private QuerySession<String[]> querySession;

    @BeforeEach
    void setup() {
        RequestContext context = new RequestContext();

        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("cdate", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("id", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.TEXT.getOID(), 2, "text", null));

        context.setDataSource("foo");
        context.setSegmentId(0);
        context.setTotalSegments(1);
        context.setTupleDescription(columnDescriptors);

        @SuppressWarnings("unchecked")
        Cache<String, QuerySession<String[]>> mockCache = mock(Cache.class);

        processor = new DemoProcessor();
        querySession = new QuerySession<>(context, mockCache);
        querySession.setProcessor(processor);
    }

    @Test
    void testDataSplitter() {
        assertSame(DemoDataSplitter.class, processor.getDataSplitter(querySession).getClass());
    }

    @Test
    void testTupleIterator() throws IOException {
        DataSplit split = new DataSplit("foo.5", new DemoFragmentMetadata("fragment5"));
        Iterator<String[]> tupleIterator = processor.getTupleIterator(querySession, split);

        assertNotNull(tupleIterator);
        assertTrue(tupleIterator.hasNext());
        assertEquals("fragment5 row1|value1|value2", tupleIterator.next());
        assertTrue(tupleIterator.hasNext());
        assertEquals("fragment5 row2|value1|value2", tupleIterator.next());
        assertFalse(tupleIterator.hasNext());
        assertThrows(NoSuchElementException.class, tupleIterator::next);
    }

//    @Test
//    void testFieldIterator() throws IOException {
//        Iterator<Object> iterator = processor.getFields(null, "fragment5 row3|value1|value2|value3|value4");
//
//        assertNotNull(iterator);
//        assertTrue(iterator.hasNext());
//        assertEquals("fragment5 row3", iterator.next());
//        assertTrue(iterator.hasNext());
//        assertEquals("value1", iterator.next());
//        assertTrue(iterator.hasNext());
//        assertEquals("value2", iterator.next());
//        assertTrue(iterator.hasNext());
//        assertEquals("value3", iterator.next());
//        assertTrue(iterator.hasNext());
//        assertEquals("value4", iterator.next());
//        assertFalse(iterator.hasNext());
//        assertThrows(NoSuchElementException.class, iterator::next);
//    }

}