package com.prakash.reports.store;

import com.prakash.reports.ReportDocument;
import com.prakash.reports.ReportJob;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Job rows and generated documents.
 * A production store would put rows in a database and documents in object storage.
 * This interface is the seam; those backends are not implemented.
 */
public interface ReportStore {
    CreateResult reserve(String userId, String idempotencyKey, Instant now) throws InterruptedException;

    void commit(UUID jobId);

    boolean abandon(UUID jobId);

    Optional<ReportJob> claim(UUID jobId, Duration lease, Instant now);

    boolean complete(UUID jobId, UUID claimToken, ReportDocument document, Instant now);

    boolean fail(UUID jobId, UUID claimToken, String error, Instant now);

    boolean requeue(UUID jobId, UUID claimToken, String error, Instant now);

    /**
     * Expired in-progress jobs go back to {@code QUEUED} while attempts remain.
     * An expired claim that already used the last attempt is marked {@code FAILED}
     * so a build that never returns cannot be retried forever.
     * Returned ids were requeued and still need {@code offerRetry}.
     */
    List<UUID> reclaimExpired(Instant now, int maxAttempts);

    Optional<ReportJob> find(UUID jobId);

    Optional<ReportJob> findByIdempotency(String userId, String idempotencyKey);

    Optional<ReportDocument> findDocument(UUID jobId);

    int inFlight(String userId);
}
