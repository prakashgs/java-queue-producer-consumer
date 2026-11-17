package com.prakash.reports;

import com.prakash.reports.queue.JobQueue;
import com.prakash.reports.store.CreateResult;
import com.prakash.reports.store.ReportStore;
import com.prakash.reports.user.UserDirectory;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Request-thread producer. Checks that the user exists, reserves a job, and offers
 * only the job id. It never builds the report.
 */
public final class ReportService {
    public static final int MAX_ID_LENGTH = 128;

    private final ReportStore store;
    private final JobQueue queue;
    private final UserDirectory directory;
    private final Clock clock;
    private final ReportPipelineConfig config;

    public ReportService(
            ReportStore store,
            JobQueue queue,
            UserDirectory directory,
            Clock clock,
            ReportPipelineConfig config
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.directory = Objects.requireNonNull(directory, "directory");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    public SubmitResult submit(String userId, String idempotencyKey) throws InterruptedException {
        if (userId == null || userId.isBlank()) {
            return SubmitResult.invalid("userId is required");
        }
        if (userId.length() > MAX_ID_LENGTH) {
            return SubmitResult.invalid("userId exceeds 128 characters");
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank() && idempotencyKey.length() > MAX_ID_LENGTH) {
            return SubmitResult.invalid("idempotencyKey exceeds 128 characters");
        }
        if (!directory.exists(userId)) {
            return SubmitResult.unknownUser();
        }
        CreateResult reserved = store.reserve(userId, idempotencyKey, clock.instant());
        return switch (reserved.kind()) {
            case EXISTING -> SubmitResult.existing(fresh(reserved.job()));
            case USER_CAP -> SubmitResult.userCap();
            case CREATED -> enqueueOrAbandon(reserved.job());
        };
    }

    public Optional<ReportJob> find(UUID jobId) {
        return store.find(jobId);
    }

    public Optional<ReportDocument> findDocument(UUID jobId) {
        return store.findDocument(jobId);
    }

    private SubmitResult enqueueOrAbandon(ReportJob job) throws InterruptedException {
        boolean offered = false;
        try {
            offered = queue.offerIncoming(job.id(), config.offerTimeout());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } finally {
            if (offered) {
                store.commit(job.id());
            } else {
                store.abandon(job.id());
            }
        }
        if (!offered) {
            return SubmitResult.queueFull();
        }
        return SubmitResult.created(fresh(job));
    }

    private ReportJob fresh(ReportJob job) {
        return store.find(job.id()).orElse(job);
    }
}
