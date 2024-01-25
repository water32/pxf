package org.greenplum.pxf.plugins.fake;

import lombok.extern.slf4j.Slf4j;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

@Slf4j
public class FakeAccessor extends BasePlugin implements Accessor {

    private static final String UNSUPPORTED_ERR_MESSAGE = "Fake accessor does not support write operation.";

    private int count = 0;

    @Override
    public boolean openForRead() throws Exception {
        return true;
    }

    @Override
    public OneRow readNextObject() throws Exception {
        if (count >= 1000000) {
            return null;

        }
        count += 1;
        return new OneRow(String.format("%d,name%d", count, count));
    }

    @Override
    public void closeForRead() throws Exception {

    }

    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }
}
