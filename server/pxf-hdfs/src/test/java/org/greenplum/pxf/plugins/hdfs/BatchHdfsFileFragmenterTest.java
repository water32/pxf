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
    public void testInitializeFilePerFragmentNotGiven() {
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(1, batchHdfsFileFragmenter.getFilesPerFragment());
    }

    @Test
    public void testInitializeFilePerFragmentGiven() {
        context.addOption(BatchHdfsFileFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "100");
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(100, batchHdfsFileFragmenter.getFilesPerFragment());
    }

    @Test
    public void testGetFragmentsFilePerFragmentNotGiven() throws Exception {
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
    public void testGetFragmentsLargerFilePerFragmentGiven() throws Exception {
        context.addOption(BatchHdfsFileFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "100");
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
    public void testGetFragmentsSmallerFilePerFragmentGiven() throws Exception {
        context.addOption(BatchHdfsFileFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "2");
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