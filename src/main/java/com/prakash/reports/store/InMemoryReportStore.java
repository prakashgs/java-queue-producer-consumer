package com.prakash.reports.store;

import com.prakash.reports.JobStatus;
import com.prakash.reports.ReportDocument;
import com.prakash.reports.ReportJob;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory jobs and documents. Not durable: everything dies with the process.
 * Completed documents are kept until the process exits.
 *
 * <p>Create and abandon share {@code createAbandonLock}. A new idempotency key is only
 * a pending reservation until {@link #commit} or {@link #abandon}. Another submit with
 * the same key waits, so it cannot be handed a job that a rejected offer is about to delete.
 * Status changes for one job go through {@code jobs.compute} on that key, so two workers
 * cannot both claim it. The in-flight slot is taken in {@code reserve} and released once,
 * on success, terminal failure, or abandon. Requeue keeps the slot.
 */
public final class InMemoryReportStore implements ReportStore {
    private final int maxInFlightPerUser;
    private final ConcurrentHashMap<UUID, ReportJob> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, ReportDocument> documents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Reservation> reservations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Boolean> slotHeld = new ConcurrentHashMap<>();
    private final ReentrantLock createAbandonLock = new ReentrantLock();
    private final Condition changed = createAbandonLock.newCondition();

    public InMemoryReportStore(int maxInFlightPerUser) {
        if (maxInFlightPerUser < 1) {
            throw new IllegalArgumentException("maxInFlightPerUser must be at least 1");
        }
        this.maxInFlightPerUser = maxInFlightPerUser;
    }

    @Override
    public CreateResult reserve(String userId, String idempotencyKey, Instant now) throws InterruptedException {
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(now, "now");
        if (userId.isBlank()) {
            throw new IllegalArgumentException("userId is blank");
        }
        String key = normalizeKey(idempotencyKey);
        String scope = scope(userId, key);
        createAbandonLock.lockInterruptibly();
        try {
            while (true) {
                if (scope != null) {
                    Reservation reservation = reservations.get(scope);
                    if (reservation != null && !reservation.committed()) {
                        try {
                            changed.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw interrupted;
                        }
                        continue;
                    }
                    if (reservation != null) {
                        ReportJob existing = jobs.get(reservation.jobId());
                        if (existing != null) {
                            return CreateResult.existing(existing);
                        }
                        reservations.remove(scope, reservation);
                    }
                }
                int held = inFlight.getOrDefault(userId, 0);
                if (held >= maxInFlightPerUser) {
                    return CreateResult.userCap();
                }
                UUID id = UUID.randomUUID();
                ReportJob job = ReportJob.queued(id, userId, key, now);
                jobs.put(id, job);
                slotHeld.put(id, Boolean.TRUE);
                inFlight.merge(userId, 1, Integer::sum);
                if (scope != null) {
                    reservations.put(scope, new Reservation(id, false));
                }
                return CreateResult.created(job);
            }
        } finally {
            createAbandonLock.unlock();
        }
    }

    @Override
    public void commit(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        createAbandonLock.lock();
        try {
            ReportJob job = jobs.get(jobId);
            if (job != null) {
                String scope = scope(job.userId(), job.idempotencyKey());
                if (scope != null) {
                    Reservation reservation = reservations.get(scope);
                    if (reservation != null && reservation.jobId().equals(jobId)) {
                        reservations.put(scope, reservation.commit());
                    }
                }
            }
            changed.signalAll();
        } finally {
            createAbandonLock.unlock();
        }
    }

    @Override
    public boolean abandon(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId");
        createAbandonLock.lock();
        try {
            ReportJob[] removed = new ReportJob[1];
            jobs.compute(jobId, (id, job) -> {
                if (job == null || job.status() != JobStatus.QUEUED) {
                    return job;
                }
                String scope = scope(job.userId(), job.idempotencyKey());
                if (scope != null) {
                    Reservation reservation = reservations.get(scope);
                    if (reservation != null && reservation.jobId().equals(jobId) && reservation.committed()) {
                        return job;
                    }
                }
                removed[0] = job;
                return null;
            });
            if (removed[0] == null) {
                return false;
            }
            String scope = scope(removed[0].userId(), removed[0].idempotencyKey());
            if (scope != null) {
                Reservation reservation = reservations.get(scope);
                if (reservation != null && reservation.jobId().equals(jobId)) {
                    reservations.remove(scope);
                }
            }
            releaseSlot(jobId, removed[0].userId());
            changed.signalAll();
            return true;
        } finally {
            createAbandonLock.unlock();
        }
    }

    @Override
    public Optional<ReportJob> claim(UUID jobId, Duration lease, Instant now) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(now, "now");
        if (lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("lease must be positive");
        }
        AtomicReference<ReportJob> claimed = new AtomicReference<>();
        jobs.compute(jobId, (id, job) -> {
            if (job == null || job.status() != JobStatus.QUEUED) {
                return job;
            }
            ReportJob next = job.claim(UUID.randomUUID(), job.attempt() + 1, now.plus(lease), now);
            claimed.set(next);
            return next;
        });
        return Optional.ofNullable(claimed.get());
    }

    @Override
    public boolean complete(UUID jobId, UUID claimToken, ReportDocument document, Instant now) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(claimToken, "claimToken");
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(now, "now");
        if (!jobId.equals(document.jobId())) {
            throw new IllegalArgumentException("document job id does not match");
        }
        AtomicBoolean applied = new AtomicBoolean();
        jobs.compute(jobId, (id, job) -> {
            if (!ownsClaim(job, claimToken)) {
                return job;
            }
            // Publish the document before the status write becomes visible.
            documents.put(jobId, document);
            releaseSlot(jobId, job.userId());
            applied.set(true);
            return job.succeed(now);
        });
        return applied.get();
    }

    @Override
    public boolean fail(UUID jobId, UUID claimToken, String error, Instant now) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(claimToken, "claimToken");
        Objects.requireNonNull(now, "now");
        AtomicBoolean applied = new AtomicBoolean();
        jobs.compute(jobId, (id, job) -> {
            if (!ownsClaim(job, claimToken)) {
                return job;
            }
            releaseSlot(jobId, job.userId());
            applied.set(true);
            return job.fail(truncate(error), now);
        });
        return applied.get();
    }

    @Override
    public boolean requeue(UUID jobId, UUID claimToken, String error, Instant now) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(claimToken, "claimToken");
        Objects.requireNonNull(now, "now");
        AtomicBoolean applied = new AtomicBoolean();
        jobs.compute(jobId, (id, job) -> {
            if (!ownsClaim(job, claimToken)) {
                return job;
            }
            applied.set(true);
            return job.requeue(truncate(error), now);
        });
        return applied.get();
    }

    @Override
    public List<UUID> reclaimExpired(Instant now, int maxAttempts) {
        Objects.requireNonNull(now, "now");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        List<UUID> requeued = new ArrayList<>();
        for (UUID id : new ArrayList<>(jobs.keySet())) {
            jobs.compute(id, (key, job) -> {
                if (job == null || job.status() != JobStatus.IN_PROGRESS || job.leaseUntil() == null) {
                    return job;
                }
                if (job.leaseUntil().isAfter(now)) {
                    return job;
                }
                if (job.attempt() >= maxAttempts) {
                    releaseSlot(key, job.userId());
                    return job.fail("lease expired", now);
                }
                requeued.add(key);
                return job.requeue("lease expired", now);
            });
        }
        return List.copyOf(requeued);
    }

    @Override
    public Optional<ReportJob> find(UUID jobId) {
        if (jobId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public Optional<ReportJob> findByIdempotency(String userId, String idempotencyKey) {
        String scope = scope(userId, normalizeKey(idempotencyKey));
        if (scope == null) {
            return Optional.empty();
        }
        Reservation reservation = reservations.get(scope);
        if (reservation == null || !reservation.committed()) {
            return Optional.empty();
        }
        return find(reservation.jobId());
    }

    @Override
    public Optional<ReportDocument> findDocument(UUID jobId) {
        if (jobId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(documents.get(jobId));
    }

    @Override
    public int inFlight(String userId) {
        if (userId == null) {
            return 0;
        }
        return inFlight.getOrDefault(userId, 0);
    }

    private void releaseSlot(UUID jobId, String userId) {
        if (slotHeld.remove(jobId) == null) {
            return;
        }
        inFlight.compute(userId, (id, count) -> {
            if (count == null || count <= 1) {
                return null;
            }
            return count - 1;
        });
    }

    private static boolean ownsClaim(ReportJob job, UUID claimToken) {
        return job != null
                && job.status() == JobStatus.IN_PROGRESS
                && claimToken.equals(job.claimToken());
    }

    private static String normalizeKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        return idempotencyKey;
    }

    private static String scope(String userId, String key) {
        if (userId == null || key == null) {
            return null;
        }
        return userId + "\0" + key;
    }

    private static String truncate(String error) {
        if (error == null || error.isBlank()) {
            return "failed";
        }
        String trimmed = error.trim();
        return trimmed.length() <= 500 ? trimmed : trimmed.substring(0, 500);
    }

    private record Reservation(UUID jobId, boolean committed) {
        private Reservation commit() {
            return new Reservation(jobId, true);
        }
    }
}
