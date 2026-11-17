package com.prakash.reports;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ReportJob(
        UUID id,
        String userId,
        String idempotencyKey,
        JobStatus status,
        int attempt,
        UUID claimToken,
        Instant createdAt,
        Instant updatedAt,
        Instant leaseUntil,
        String error
) {
    public ReportJob {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be >= 0");
        }
        if (status == JobStatus.QUEUED && claimToken != null) {
            throw new IllegalArgumentException("queued job cannot hold a claim token");
        }
        if (status == JobStatus.IN_PROGRESS) {
            Objects.requireNonNull(claimToken, "claimToken");
            Objects.requireNonNull(leaseUntil, "leaseUntil");
        }
    }

    public static ReportJob queued(UUID id, String userId, String idempotencyKey, Instant now) {
        return new ReportJob(id, userId, idempotencyKey, JobStatus.QUEUED, 0, null, now, now, null, null);
    }

    public ReportJob claim(UUID token, int nextAttempt, Instant leaseUntil, Instant now) {
        return new ReportJob(
                id,
                userId,
                idempotencyKey,
                JobStatus.IN_PROGRESS,
                nextAttempt,
                token,
                createdAt,
                now,
                leaseUntil,
                null);
    }

    public ReportJob requeue(String reason, Instant now) {
        return new ReportJob(
                id,
                userId,
                idempotencyKey,
                JobStatus.QUEUED,
                attempt,
                null,
                createdAt,
                now,
                null,
                reason);
    }

    public ReportJob succeed(Instant now) {
        return new ReportJob(
                id,
                userId,
                idempotencyKey,
                JobStatus.SUCCEEDED,
                attempt,
                claimToken,
                createdAt,
                now,
                null,
                null);
    }

    public ReportJob fail(String reason, Instant now) {
        return new ReportJob(
                id,
                userId,
                idempotencyKey,
                JobStatus.FAILED,
                attempt,
                claimToken,
                createdAt,
                now,
                null,
                reason);
    }
}
