package org.greenplum.pxf.api;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// TODO: convert to a @Component
public class ExecutorServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceProvider.class);

    public static final int MACHINE_CORES = Runtime.getRuntime().availableProcessors();

    public static final int THREAD_POOL_SIZE = MACHINE_CORES * 10;

    public static final ThreadFactory NAMED_THREAD_FACTORY =
        new ThreadFactoryBuilder().setNameFormat("pxf-worker-%d").build();

    public static final ExecutorService EXECUTOR_SERVICE = MoreExecutors.getExitingExecutorService(
        new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE,
            1, TimeUnit.SECONDS, new PriorityBlockingQueue<>(1000,
            (o1, o2) -> ThreadLocalRandom.current().nextInt(-100, 100)),
            NAMED_THREAD_FACTORY, new ThreadPoolExecutor.CallerRunsPolicy()));

    public static ExecutorService get() {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }
}
