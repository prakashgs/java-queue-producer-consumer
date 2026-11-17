package com.prakash.reports;

import java.time.Duration;
import java.util.Objects;

/**
 * Runtime knobs for the in-process pipeline.
 *
 * <p>Steady-state throughput is about {@code workerCount / 2} reports per second
 * when each build takes 2 seconds, because a worker is busy for the whole build.
 */
public record ReportPipelineConfig(
        int incomingCapacity,
        int workerCount,
        int maxAttempts,
        int maxInFlightPerUser,
        Duration offerTimeout,
        Duration lease,
        Duration pollTimeout,
        Duration sweepInterval
) {
    /**
     * Capacity 1000, at least 4 workers, 3 attempts, 8 in-flight jobs per user,
     * a 25ms offer timeout, and a 30 second claim lease.
     * A zero sweep interval disables the background sweeper; callers can still sweep once.
     */
    public static ReportPipelineConfig defaults() {
        return new ReportPipelineConfig(
                1000,
                4,
                3,
                8,
                Duration.ofMillis(25),
                Duration.ofSeconds(30),
                Duration.ofMillis(200),
                Duration.ofSeconds(1));
    }

    public ReportPipelineConfig {
        if (incomingCapacity < 1) {
            throw new IllegalArgumentException("incomingCapacity must be at least 1");
        }
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be at least 1");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (maxInFlightPerUser < 1) {
            throw new IllegalArgumentException("maxInFlightPerUser must be at least 1");
        }
        requireNonNegative(offerTimeout, "offerTimeout");
        requirePositive(lease, "lease");
        requireNonNegative(pollTimeout, "pollTimeout");
        requireNonNegative(sweepInterval, "sweepInterval");
    }

    private static void requirePositive(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireNonNegative(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
