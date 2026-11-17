package com.prakash.reports;

import com.prakash.reports.queue.JobQueue;
import com.prakash.reports.store.ReportStore;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Returns expired in-progress jobs to the retry queue. A zero sweep interval leaves this manual. */
public final class LeaseSweeper implements AutoCloseable {
    private final ReportPipelineConfig config;
    private final ReportStore store;
    private final JobQueue queue;
    private final Clock clock;
    private volatile boolean running;
    private Thread thread;

    public LeaseSweeper(ReportPipelineConfig config, ReportStore store, JobQueue queue, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public synchronized void start() {
        if (thread != null || config.sweepInterval().isZero()) {
            return;
        }
        running = true;
        thread = new Thread(this::runLoop, "report-lease-sweeper");
        thread.setDaemon(false);
        thread.start();
    }

    public void sweepOnce() {
        List<UUID> requeued = store.reclaimExpired(clock.instant(), config.maxAttempts());
        for (UUID jobId : requeued) {
            queue.offerRetry(jobId);
        }
    }

    private void runLoop() {
        while (running) {
            sweepOnce();
            try {
                Thread.sleep(Math.max(1, config.sweepInterval().toMillis()));
            } catch (InterruptedException interrupted) {
                if (!running) {
                    break;
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        Thread current = thread;
        if (current == null) {
            return;
        }
        current.interrupt();
        try {
            current.join();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
