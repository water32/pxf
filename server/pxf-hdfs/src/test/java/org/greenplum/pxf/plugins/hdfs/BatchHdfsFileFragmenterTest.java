package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BatchHdfsFileFragmenterTest {

    private BatchHdfsFileFragmenter batchHdfsFileFragmenter;
    private List<Fragment> fragments;
    private List<Fragment> correctFragments;
    private RequestContext context;
    private String path;

    @Before
    public void setup() {
        batchHdfsFileFragmenter = new BatchHdfsFileFragmenter();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("user");
        path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("csv/")).getPath();
        context.setProfileScheme("localfile");
        context.setDataSource(path);
    }

    @Test
    public void testInitializeBatchSizeNotGiven() {
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(1, batchHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testInitializeBatchSizeGiven() {
        context.addOption("BATCH_SIZE", "100");
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(100, batchHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testGetFragmentsBatchSizeNotGiven() throws Exception {
        batchHdfsFileFragmenter.initialize(context);
        fragments = batchHdfsFileFragmenter.getFragments();
        correctFragments = new ArrayList<>();
        correctFragments.add(new Fragment("file://" + path + "empty.csv"));
        correctFragments.add(new Fragment("file://" + path + "quoted.csv"));
        correctFragments.add(new Fragment("file://" + path + "simple.csv"));
        correctFragments.add(new Fragment("file://" + path + "singleline.csv"));

        assertNotNull(fragments);
        assertEquals(4, fragments.size());
        assertFragmentListEquals(correctFragments, fragments);
    }

    @Test
    public void testGetFragmentsLargerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "100");
        batchHdfsFileFragmenter.initialize(context);
        fragments = batchHdfsFileFragmenter.getFragments();
        correctFragments = new ArrayList<>();
        correctFragments.add(new Fragment(
                "file://" + path + "empty.csv" + ","
                        + "file://" + path + "quoted.csv" + ","
                        + "file://" + path + "simple.csv" + ","
                        + "file://" + path + "singleline.csv" // correctly sorted order
        ));


        assertNotNull(fragments);
        assertEquals(1, fragments.size());
        assertFragmentListEquals(correctFragments, fragments);
    }

    @Test
    public void testGetFragmentsSmallerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "2");
        batchHdfsFileFragmenter.initialize(context);
        fragments = batchHdfsFileFragmenter.getFragments();
        correctFragments = new ArrayList<>();
        correctFragments.add(new Fragment(
                "file://" + path + "empty.csv" + ","
                        + "file://" + path + "quoted.csv"
        ));
        correctFragments.add(new Fragment(
                "file://" + path + "simple.csv" + ","
                        + "file://" + path + "singleline.csv"
        ));

        assertNotNull(fragments);
        assertEquals(2, fragments.size());
        assertFragmentListEquals(correctFragments, fragments);
    }

    private static void assertFragmentListEquals(List<Fragment> correctFragments, List<Fragment> fragments) {
        int cnt = 0;
        for (Fragment fragment : correctFragments) {
            assertEquals(fragment.getSourceName(), fragments.get(cnt++).getSourceName());
        }
    }
}