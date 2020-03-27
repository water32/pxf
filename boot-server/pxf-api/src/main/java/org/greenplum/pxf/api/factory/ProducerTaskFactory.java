package org.greenplum.pxf.api.factory;

import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.task.ProducerTask;
import org.springframework.stereotype.Component;

@Component
public class ProducerTaskFactory<T, M> {

    public ProducerTask<T, M> getProducerTask(QuerySession querySession) {
        return new ProducerTask<T, M>(querySession);
    }
}
