package org.greenplum.pxf.api.concurrent;

import com.google.common.base.Preconditions;
import org.greenplum.pxf.api.task.TupleReaderTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Source: io.airlift.concurrent.BoundedExecutor
 * <p>
 * Guarantees that no more than maxThreads will be used to execute tasks submitted
 * through {@link #execute(Runnable) execute()}.
 * <p>
 * There are a few interesting properties:
 * <ul>
 * <li>Multiple BoundedExecutors over a single coreExecutor will have fair sharing
 * of the coreExecutor threads proportional to their relative maxThread counts, but
 * can use less if not as active.</li>
 * <li>Tasks submitted to a BoundedExecutor is guaranteed to have those tasks
 * handed to threads in that order.</li>
 * <li>Will not encounter starvation</li>
 * </ul>
 */
// TODO: do we want to make BoundedExecutor implement ExecutorService, otherwise
//       we should just add the airlift library as a dependency and use BoundedExecutor from there
public class BoundedExecutor
        extends AbstractExecutorService {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedExecutor.class);

    private final List<TupleReaderTask<?>> tasks;
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicBoolean failed = new AtomicBoolean();

    private final Executor coreExecutor;
    private final int maxThreads;

    private volatile boolean isShutdown;

    public BoundedExecutor(Executor coreExecutor, int maxThreads) {
        requireNonNull(coreExecutor, "coreExecutor is null");
        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.coreExecutor = coreExecutor;
        this.maxThreads = maxThreads;
        this.isShutdown = false;
        this.tasks = new CopyOnWriteArrayList<>();
    }

    @Override
    public void execute(Runnable task) {
        if (failed.get()) {
            throw new RejectedExecutionException("BoundedExecutor is in a failed state");
        }

        if (isShutdown) {
            return;
        }

        queue.add(task);

        int size = queueSize.incrementAndGet();
        if (size <= maxThreads) {
            // If able to grab a permit (aka size <= maxThreads), then we are short exactly one draining thread
            try {
                coreExecutor.execute(this::drainQueue);
            } catch (Throwable e) {
                failed.set(true);
                LOG.error("BoundedExecutor state corrupted due to underlying executor failure");
                throw e;
            }
        }
    }

    private void drainQueue() {
        // INVARIANT: queue has at least one task available when this method is called
        do {
            Runnable task = null;
            try {
                task = queue.poll();
                tasks.add((TupleReaderTask<?>) task);
                requireNonNull(task).run();
            } catch (Throwable e) {
                LOG.error("Task failed", e);
            } finally {
                if (task != null) {
                    tasks.remove(task);
                }
            }
        } while (queueSize.getAndDecrement() > maxThreads && !isShutdown);
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Runnable> shutdownNow() {
        isShutdown = true;
        // set state that is shutdown
        Queue<Runnable> q = queue;
        Runnable[] tasks = new Runnable[q.size()];
        q.toArray(tasks);
        q.clear();
        queueSize.addAndGet(-tasks.length);

        Arrays.stream(tasks).forEach(t -> t.);
        return Arrays.asList(tasks);
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public boolean isTerminated() {
        return queueSize.get() == 0 && isShutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }
}
