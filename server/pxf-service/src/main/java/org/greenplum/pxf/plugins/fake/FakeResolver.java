package org.greenplum.pxf.plugins.fake;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;

import java.util.Collections;
import java.util.List;

public class FakeResolver extends BasePlugin implements Resolver {
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        return Collections.singletonList(new OneField(DataType.VARCHAR.getOID(), row.getData()));
    }

    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        throw new UnsupportedOperationException("Fake resolver does not support write operation.");
    }
}
