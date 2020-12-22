package org.greenplum.pxf.api.concurrent;

import lombok.SneakyThrows;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentLinkedBlockingDeque<E> extends ConcurrentLinkedDeque<E> {

    /**
     * Maximum number of items in the deque
     */
    private final int capacity;

    /**
     * Number of items in the deque
     */
    private final AtomicInteger count;

    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition for waiting takes
     */
    private final Condition notEmpty = lock.newCondition();

    /**
     * Condition for waiting puts
     */
    private final Condition notFull = lock.newCondition();


    /**
     * Creates a {@code ConcurrentLinkedBlockingDeque} with the given (fixed) capacity.
     *
     * @param capacity the capacity of this deque
     * @throws IllegalArgumentException if {@code capacity} is less than 1
     */
    public ConcurrentLinkedBlockingDeque(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException();
        this.capacity = capacity;
        this.count = new AtomicInteger(0);
    }

    @SneakyThrows
    @Override
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();

        if (count.get() >= capacity) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                while (count.get() >= capacity)
                    notFull.await();
            } finally {
                lock.unlock();
            }
        }

        count.incrementAndGet();
        boolean r = super.offer(e);
        notEmpty.signal();
        return r;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        if (count.get() >= capacity) {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                E x;
                while ((x = super.poll()) == null) {
                    if (nanos <= 0)
                        return null;
                    nanos = notEmpty.awaitNanos(nanos);
                }
                count.decrementAndGet();
                notFull.signal();
                return x;
            } finally {
                lock.unlock();
            }
        } else {
            count.decrementAndGet();
            return super.poll();
        }
    }
}
