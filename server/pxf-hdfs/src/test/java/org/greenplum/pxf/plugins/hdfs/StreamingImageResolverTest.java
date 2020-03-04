package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class StreamingImageResolverTest {
    StreamingImageResolver resolver;
    List<String> paths;
    List<String> parentDirs;
    List<String> fileNames;
    List<Integer> dimensions;
    List<BufferedImage> images;
    List<byte[]> image_byteas;
    private static final int NUM_IMAGES = 5;
    OneRow row;
    RequestContext context;
    @Mock
    StreamingImageAccessor accessor;
    List<String> imageStrings;

    @Before
    public void setup() throws IOException, InterruptedException {
        context = new RequestContext();
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 0, "foo", null));
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 1, "foo", null));
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 2, "foo", null));
            add(new ColumnDescriptor("foo", DataType.INT8ARRAY.getOID(), 3, "foo", null));
            add(new ColumnDescriptor("foo", DataType.INT8ARRAY.getOID(), 4, "foo", null));
        }});
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        resolver = new StreamingImageResolver();
        paths = new ArrayList<String>() {{
            add("/foo/parentDir1/image1.png");
            add("/foo/parentDir2/image2.png");
            add("/foo/parentDir3/image3.png");
            add("/foo/parentDir4/image4.png");
            add("/foo/parentDir5/image5.png");
        }};
        parentDirs = new ArrayList<String>() {{
            add("parentDir1");
            add("parentDir2");
            add("parentDir3");
            add("parentDir4");
            add("parentDir5");
        }};
        fileNames = new ArrayList<String>() {{
            add("image1.png");
            add("image2.png");
            add("image3.png");
            add("image4.png");
            add("image5.png");
        }};
        dimensions = new ArrayList<Integer>() {{
            add(5);
            add(2);
            add(2);
        }};
        images = ImageTestHelper.generateMonocolorImages(2, 2, new int[]{1, 5, 10, 50, 100});
        imageStrings = new ArrayList<String>() {{
            add("{{{1,1,1},{1,1,1}},{{1,1,1},{1,1,1}}}");
            add("{{{5,5,5},{5,5,5}},{{5,5,5},{5,5,5}}}");
            add("{{{10,10,10},{10,10,10}},{{10,10,10},{10,10,10}}}");
            add("{{{50,50,50},{50,50,50}},{{50,50,50},{50,50,50}}}");
            add("{{{100,100,100},{100,100,100}},{{100,100,100},{100,100,100}}}");
        }};
        image_byteas = new ArrayList<byte[]>() {{
            add(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
            add(new byte[]{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5});
            add(new byte[]{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10});
            add(new byte[]{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50});
            add(new byte[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100});
        }};
        doAnswer(new Answer<Object>() {
            int cnt = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                return cnt == NUM_IMAGES ? null : images.get(cnt++);
            }
        }).when(accessor).next();
        doAnswer(new Answer<Object>() {
            int cnt = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                return cnt++ != NUM_IMAGES;
            }
        }).when(accessor).hasNext();
        row = new OneRow(paths, accessor);
        resolver.initialize(context);
    }

    @Test
    public void testGetFields() throws IOException, InterruptedException {
        List<OneField> fields = resolver.getFields(row);

        assertEquals(5, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(3)).val instanceof List);
        assertTrue(fields.get(4) instanceof ArrayStreamingField);
        assertTrue(((ArrayStreamingField) fields.get(4)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(paths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(2)).val);
        assertListEquals(dimensions, (List<?>) ((ArrayField) fields.get(3)).val);
        assertEquals(resolver, ((ArrayStreamingField) fields.get(4)).getResolver());
        assertImages();
    }

    @Test
    public void testGetFields_byteArray() throws IOException, InterruptedException {
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 0, "foo", null));
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 1, "foo", null));
            add(new ColumnDescriptor("foo", DataType.TEXTARRAY.getOID(), 2, "foo", null));
            add(new ColumnDescriptor("foo", DataType.INT8ARRAY.getOID(), 3, "foo", null));
            add(new ColumnDescriptor("foo", DataType.BYTEA.getOID(), 4, "foo", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(5, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(3)).val instanceof List);
        assertTrue(fields.get(4) instanceof StreamingField);
        assertTrue(((StreamingField) fields.get(4)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(paths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(2)).val);
        assertListEquals(dimensions, (List<?>) ((ArrayField) fields.get(3)).val);
        assertEquals(resolver, ((StreamingField) fields.get(4)).getResolver());
        assertImages();
    }

    @Test
    public void testGetNextAndHasNext() throws IOException, InterruptedException {
        resolver.getFields(row);
        for (String image : imageStrings) {
            assertTrue(resolver.hasNext());
            assertEquals(image, resolver.next());
        }
        assertFalse(resolver.hasNext());
        assertNull(resolver.next());
    }

    private void assertListEquals(List<?> correct, List<?> val) {
        int cnt = 0;
        for (Object c : correct) {
            assertEquals(c, val.get(cnt++));
        }
    }

    private void assertImages() throws IOException, InterruptedException {
        int cnt = 0;
        while (resolver.hasNext()) {
            Object o = resolver.next();
            if (cnt == NUM_IMAGES) {
                assertNull(o);
                return;
            } else if (o instanceof String) {
                assertEquals(imageStrings.get(cnt++), o);
            } else {
                assertTrue(o instanceof byte[]);
                assertArrayEquals(image_byteas.get(cnt++), (byte[]) o);
            }
        }
    }
}