package com.prakash.reports;

import com.prakash.reports.build.ReportBuilder;
import com.prakash.reports.queue.JobQueue;
import com.prakash.reports.store.ReportStore;
import com.prakash.reports.user.UserDirectory;
import com.prakash.reports.user.UserProfile;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Claims a queued job, loads the profile, and builds the report.
 * A failed build is offered to the unbounded retry queue, never back onto the bounded incoming queue.
 * Complete and fail carry the claim token, so a stale worker is a no-op after the lease moves on.
 */
public final class ReportProcessor {
    private final ReportStore store;
    private final JobQueue queue;
    private final UserDirectory directory;
    private final ReportBuilder builder;
    private final Clock clock;
    private final ReportPipelineConfig config;

    public ReportProcessor(
            ReportStore store,
            JobQueue queue,
            UserDirectory directory,
            ReportBuilder builder,
            Clock clock,
            ReportPipelineConfig config
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.directory = Objects.requireNonNull(directory, "directory");
        this.builder = Objects.requireNonNull(builder, "builder");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    public void process(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        Optional<ReportJob> claimed = store.claim(jobId, config.lease(), clock.instant());
        if (claimed.isEmpty()) {
            return;
        }
        ReportJob job = claimed.get();
        try {
            UserProfile profile = directory.load(job.userId());
            ReportDocument document = builder.build(profile, job.id(), clock.instant());
            store.complete(job.id(), job.claimToken(), document, clock.instant());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            settleFailure(job, "interrupted");
        } catch (Exception failure) {
            String message = failure.getMessage();
            settleFailure(job, message == null || message.isBlank() ? failure.getClass().getSimpleName() : message);
        }
    }

    private void settleFailure(ReportJob job, String error) {
        if (job.attempt() >= config.maxAttempts()) {
            store.fail(job.id(), job.claimToken(), error, clock.instant());
            return;
        }
        if (store.requeue(job.id(), job.claimToken(), error, clock.instant())) {
            queue.offerRetry(job.id());
        }
    }
}
