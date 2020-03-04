package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class StreamingImageResolverTest {
    StreamingImageResolver resolver;
    List<String> paths;
    List<String> parentDirs;
    List<String> fileNames;
    List<BufferedImage> images;
    private static final int NUM_IMAGES = 5;
    OneRow row;
    @Mock
    StreamingImageAccessor accessor;
    List<String> imageStrings;

    @Before
    public void setup() throws IOException {
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
        images = ImageTestHelper.generateMonocolorImages(2, 2, new int[]{1, 5, 10, 50, 100});
        imageStrings = new ArrayList<String>() {{
            add("{{{1,1,1},{1,1,1}},{{1,1,1},{1,1,1}}}");
            add("{{{5,5,5},{5,5,5}},{{5,5,5},{5,5,5}}}");
            add("{{{10,10,10},{10,10,10}},{{10,10,10},{10,10,10}}}");
            add("{{{50,50,50},{50,50,50}},{{50,50,50},{50,50,50}}}");
            add("{{{100,100,100},{100,100,100}},{{100,100,100},{100,100,100}}}");
        }};
        doAnswer(new Answer<Object>() {
            int cnt = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (cnt == NUM_IMAGES) {
                    return null;
                }
                return images.get(cnt++);
            }
        }).when(accessor).readNextImage();
        row = new OneRow(paths, accessor);
    }

    @Test
    public void testGetFields() {
        List<OneField> fields = resolver.getFields(row);

        assertEquals(4, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertTrue(fields.get(3) instanceof ArrayStreamingField);
        assertTrue(((ArrayStreamingField) fields.get(3)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(paths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(2)).val);
        assertEquals(resolver, ((ArrayStreamingField) fields.get(3)).getResolver());
    }

    @Test
    public void testGetNext() {
        resolver.getFields(row);
        for (String image : imageStrings) {
            assertEquals(image, resolver.next());
        }
        assertNull(resolver.next());
    }

    @Test
    public void testHasNext() {
        resolver.getFields(row);
        resolver.hasNext();
        verify(accessor, times(1)).hasNext();
    }

    private void assertListEquals(List<String> correct, List<?> val) {
        int cnt = 0;
        for (String c : correct) {
            assertEquals(c, val.get(cnt++));
        }
    }

}