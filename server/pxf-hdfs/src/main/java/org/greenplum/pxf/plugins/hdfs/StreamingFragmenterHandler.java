package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;

public class StreamingFragmenterHandler implements ProtocolHandler {
    private static final String STREAMING_FRAGMENTER = StreamingHdfsFileFragmenter.class.getName();

    @Override
    public String getFragmenterClassName(RequestContext context) {
        final String streamFragments = context.getOption("STREAM_FRAGMENTS");
        if (streamFragments != null && streamFragments.toLowerCase().equals("true")) {
            return STREAMING_FRAGMENTER;
        }
        return context.getFragmenter();
    }

    @Override
    public String getAccessorClassName(RequestContext context) {
        return context.getAccessor();
    }

    @Override
    public String getResolverClassName(RequestContext context) {
        return context.getResolver();
    }
}
