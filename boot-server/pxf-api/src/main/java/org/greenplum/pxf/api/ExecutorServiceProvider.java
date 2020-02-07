package org.greenplum.pxf.api;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.greenplum.pxf.api.task.TupleReaderTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceProvider.class);

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores

    public static final int MACHINE_CORES = Runtime.getRuntime().availableProcessors();

    public static final int THREAD_POOL_SIZE = MACHINE_CORES * 10;

    public static final ThreadFactory NAMED_THREAD_FACTORY =
            new ThreadFactoryBuilder().setNameFormat("pxf-worker-%d").build();

    public static final ExecutorService EXECUTOR_SERVICE = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE,
                    1, TimeUnit.SECONDS, new PriorityBlockingQueue<>(1000, new Comparator<Runnable>() {
                @Override
                public int compare(Runnable o1, Runnable o2) {
                    if (o1 instanceof TupleReaderTask && o2 instanceof TupleReaderTask) {
                        TupleReaderTask t1 = (TupleReaderTask) o1;
                        TupleReaderTask t2 = (TupleReaderTask) o2;
                        return Integer.compare(t1.getOutputQueueSize(), t2.getOutputQueueSize());
                    } else {
                        LOG.error("This should not happen o1 {} o2 {}", o1.getClass().getName(), o2.getClass().getName());
                    }
                    return 0;
                }
            }),
                    NAMED_THREAD_FACTORY, new ThreadPoolExecutor.CallerRunsPolicy()));

    public static ExecutorService get() {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
