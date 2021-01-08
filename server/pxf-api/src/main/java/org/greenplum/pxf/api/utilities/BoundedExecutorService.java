//package org.greenplum.pxf.api.utilities;
//
//import com.google.common.base.Preconditions;
//import org.greenplum.pxf.api.task.TupleReaderTask;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.List;
//import java.util.Queue;
//import java.util.concurrent.AbstractExecutorService;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.Executor;
//import java.util.concurrent.RejectedExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.Condition;
//import java.util.concurrent.locks.ReentrantLock;
//
//import static java.util.Objects.requireNonNull;
//
///**
// * Source: io.airlift.concurrent.BoundedExecutorService
// * <p>
// * Guarantees that no more than maxThreads will be used to execute tasks submitted
// * through {@link #execute(Runnable) execute()}.
// * <p>
// * There are a few interesting properties:
// * <ul>
// * <li>Multiple BoundedExecutors over a single coreExecutor will have fair sharing
// * of the coreExecutor threads proportional to their relative maxThread counts, but
// * can use less if not as active.</li>
// * <li>Tasks submitted to a BoundedExecutorService is guaranteed to have those tasks
// * handed to threads in that order.</li>
// * <li>Will not encounter starvation</li>
// * </ul>
// */
//public class BoundedExecutorService
//        extends AbstractExecutorService {
//    private static final Logger LOG = LoggerFactory.getLogger(BoundedExecutorService.class);
//
//    private final Collection<TupleReaderTask<?>> runningTasks;
//    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
//    private final AtomicInteger queueSize = new AtomicInteger(0);
//    private final AtomicBoolean failed = new AtomicBoolean();
//
//    /**
//     * Synchronization lock for notification.
//     */
//    private final ReentrantLock notificationLock = new ReentrantLock();
//
//    /**
//     * Wait until there are no active segments
//     */
//    private final Condition notFull = notificationLock.newCondition();
//
//    private final Executor coreExecutor;
//    private final int maxThreads;
//
//    private volatile boolean isShutdown;
//
//    public BoundedExecutorService(Executor coreExecutor, int maxThreads) {
//        requireNonNull(coreExecutor, "coreExecutor is null");
//        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
//        this.coreExecutor = coreExecutor;
//        this.maxThreads = maxThreads;
//        this.isShutdown = false;
//        this.runningTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
//    }
//
//    @Override
//    public void execute(Runnable task) {
//        if (failed.get()) {
//            throw new RejectedExecutionException("BoundedExecutorService is in a failed state");
//        }
//
//        if (isShutdown) {
//            return;
//        }
//
//        queue.add(task);
//
//        int size = queueSize.incrementAndGet();
//        if (size <= maxThreads) {
//            // If able to grab a permit (aka size <= maxThreads), then we are short exactly one draining thread
//            try {
//                coreExecutor.execute(this::drainQueue);
//            } catch (Throwable e) {
//                failed.set(true);
//                LOG.error("BoundedExecutorService state corrupted due to underlying executor failure");
//                throw e;
//            }
//        }
//    }
//
//    private void drainQueue() {
//        // INVARIANT: queue has at least one task available when this method is called
//        do {
//            Runnable task = null;
//            try {
//                task = requireNonNull(queue.poll());
//                runningTasks.add((TupleReaderTask<?>) task);
//                task.run();
//            } catch (Throwable e) {
//                LOG.error("Task failed", e);
//            } finally {
//                if (task != null) {
//                    runningTasks.remove(task);
//                }
//            }
//        } while (queueSize.getAndDecrement() > maxThreads && !isShutdown);
//        signalNotFull();
//    }
//
//    @Override
//    public void shutdown() {
//        isShutdown = true;
//    }
//
//    @Override
//    public List<Runnable> shutdownNow() {
//        isShutdown = true;
//        cancelRunningTasks();
//        Runnable[] queuedTasks = cancelQueuedTasks();
//        return Arrays.asList(queuedTasks);
//    }
//
//    @Override
//    public boolean isShutdown() {
//        return isShutdown;
//    }
//
//    @Override
//    public boolean isTerminated() {
//        return runningTasks.isEmpty() && isShutdown;
//    }
//
//    @Override
//    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
//        final ReentrantLock notificationLock = this.notificationLock;
//        notificationLock.lockInterruptibly();
//        try {
//            while (!isTerminated()) {
//                notFull.await(timeout, unit);
//            }
//        } finally {
//            notificationLock.unlock();
//        }
//        return true;
//    }
//
//    private void cancelRunningTasks() {
//        runningTasks.forEach(TupleReaderTask::cancel);
//    }
//
//    private Runnable[] cancelQueuedTasks() {
//        Queue<Runnable> q = queue;
//        Runnable[] queuedTasks = new Runnable[q.size()];
//        q.toArray(queuedTasks);
//        q.clear();
//        queueSize.addAndGet(-queuedTasks.length);
//        Arrays.stream(queuedTasks).forEach(t -> ((TupleReaderTask<?>) t).cancel());
//        return queuedTasks;
//    }
//
//    /**
//     * Signals a waiting take. Called only from put/offer (which do not
//     * otherwise ordinarily lock takeLock.)
//     */
//    private void signalNotFull() {
//        final ReentrantLock notificationLock = this.notificationLock;
//        notificationLock.lock();
//        try {
//            notFull.signal();
//        } finally {
//            notificationLock.unlock();
//        }
//    }
//}
