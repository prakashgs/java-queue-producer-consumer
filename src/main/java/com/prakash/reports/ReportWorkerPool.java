package com.prakash.reports;

import com.prakash.reports.queue.JobQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Takes job ids and runs the processor. {@link #close()} stops polling and waits for
 * the build already in progress. It does not interrupt that build.
 */
public final class ReportWorkerPool implements AutoCloseable {
    private final ReportPipelineConfig config;
    private final JobQueue queue;
    private final ReportProcessor processor;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;
    private boolean started;

    public ReportWorkerPool(ReportPipelineConfig config, JobQueue queue, ReportProcessor processor) {
        this.config = Objects.requireNonNull(config, "config");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        running = true;
        for (int i = 0; i < config.workerCount(); i++) {
            Thread thread = new Thread(this::runLoop, "report-worker-" + i);
            thread.setDaemon(false);
            threads.add(thread);
            thread.start();
        }
    }

    private void runLoop() {
        while (running) {
            UUID jobId;
            try {
                jobId = queue.poll(config.pollTimeout());
            } catch (InterruptedException interrupted) {
                if (!running) {
                    break;
                }
                continue;
            }
            if (jobId == null) {
                continue;
            }
            processor.process(jobId);
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        queue.stopWaiting();
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
