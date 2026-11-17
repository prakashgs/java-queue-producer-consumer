package com.prakash.reports.queue;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One lock guards both deques so offer and poll cannot deadlock on two queue locks.
 * Retry is drained first. {@code offerRetry} never waits.
 */
public final class InMemoryJobQueue implements JobQueue {
    private final int capacity;
    private final ArrayDeque<UUID> incoming = new ArrayDeque<>();
    private final ArrayDeque<UUID> retry = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition space = lock.newCondition();
    private final Condition available = lock.newCondition();
    private boolean accepting = true;

    public InMemoryJobQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1");
        }
        this.capacity = capacity;
    }

    @Override
    public boolean offerIncoming(UUID jobId, Duration timeout) throws InterruptedException {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(timeout, "timeout");
        long deadline = System.nanoTime() + Math.max(0, timeout.toNanos());
        lock.lockInterruptibly();
        try {
            while (incoming.size() >= capacity) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                space.awaitNanos(remaining);
            }
            incoming.addLast(jobId);
            available.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void offerRetry(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        lock.lock();
        try {
            retry.addLast(jobId);
            available.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public UUID poll(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        long deadline = System.nanoTime() + Math.max(0, timeout.toNanos());
        lock.lockInterruptibly();
        try {
            while (accepting) {
                UUID jobId = pollReady();
                if (jobId != null) {
                    return jobId;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                available.awaitNanos(remaining);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stopWaiting() {
        lock.lock();
        try {
            accepting = false;
            available.signalAll();
            space.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int incomingDepth() {
        lock.lock();
        try {
            return incoming.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int retryDepth() {
        lock.lock();
        try {
            return retry.size();
        } finally {
            lock.unlock();
        }
    }

    private UUID pollReady() {
        UUID jobId = retry.pollFirst();
        if (jobId != null) {
            return jobId;
        }
        jobId = incoming.pollFirst();
        if (jobId != null) {
            space.signal();
        }
        return jobId;
    }
}
