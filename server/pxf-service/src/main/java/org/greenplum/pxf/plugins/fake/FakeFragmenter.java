package org.greenplum.pxf.plugins.fake;

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.Fragmenter;

import java.util.Collections;
import java.util.List;

public class FakeFragmenter extends BasePlugin implements Fragmenter {
    @Override
    public List<Fragment> getFragments() throws Exception {
        return Collections.singletonList(new Fragment(context.getDataSource()));
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for Fake plugin is not supported");
    }
}
