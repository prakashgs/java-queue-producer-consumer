package com.prakash.reports;

import com.prakash.reports.build.ReportBuilder;
import com.prakash.reports.queue.InMemoryJobQueue;
import com.prakash.reports.queue.JobQueue;
import com.prakash.reports.store.InMemoryReportStore;
import com.prakash.reports.store.ReportStore;
import com.prakash.reports.user.UserDirectory;

import java.time.Clock;
import java.util.Objects;

/**
 * Wires the in-memory store, the split queue, the worker pool, and the lease sweeper.
 * {@link #close()} stops new work and lets the current build finish.
 */
public final class ReportPipeline implements AutoCloseable {
    private final ReportService service;
    private final ReportStore store;
    private final JobQueue queue;
    private final ReportProcessor processor;
    private final ReportWorkerPool workers;
    private final LeaseSweeper sweeper;
    private boolean closed;

    private ReportPipeline(
            ReportService service,
            ReportStore store,
            JobQueue queue,
            ReportProcessor processor,
            ReportWorkerPool workers,
            LeaseSweeper sweeper
    ) {
        this.service = service;
        this.store = store;
        this.queue = queue;
        this.processor = processor;
        this.workers = workers;
        this.sweeper = sweeper;
    }

    public static ReportPipeline create(
            ReportPipelineConfig config,
            UserDirectory users,
            ReportBuilder builder,
            Clock clock
    ) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(users, "users");
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(clock, "clock");
        InMemoryReportStore store = new InMemoryReportStore(config.maxInFlightPerUser());
        InMemoryJobQueue queue = new InMemoryJobQueue(config.incomingCapacity());
        ReportService service = new ReportService(store, queue, users, clock, config);
        ReportProcessor processor = new ReportProcessor(store, queue, users, builder, clock, config);
        ReportWorkerPool workers = new ReportWorkerPool(config, queue, processor);
        LeaseSweeper sweeper = new LeaseSweeper(config, store, queue, clock);
        return new ReportPipeline(service, store, queue, processor, workers, sweeper);
    }

    public ReportPipeline start() {
        workers.start();
        sweeper.start();
        return this;
    }

    public ReportService service() {
        return service;
    }

    public ReportStore store() {
        return store;
    }

    public JobQueue queue() {
        return queue;
    }

    public ReportProcessor processor() {
        return processor;
    }

    public void sweepOnce() {
        sweeper.sweepOnce();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            workers.close();
            sweeper.close();
        }
    }
}
