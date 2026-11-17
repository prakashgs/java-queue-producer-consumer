package com.prakash.reports.build;

import com.prakash.reports.ReportDocument;
import com.prakash.reports.user.UserProfile;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Blocks for the injected duration, then writes a short activity report. Tests supply a no-op sleeper. */
public final class SleepingReportBuilder implements ReportBuilder {
    private final Duration buildTime;
    private final Sleeper sleeper;

    public SleepingReportBuilder(Duration buildTime, Sleeper sleeper) {
        if (buildTime == null || buildTime.isNegative()) {
            throw new IllegalArgumentException("buildTime must be zero or positive");
        }
        this.buildTime = buildTime;
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    @Override
    public ReportDocument build(UserProfile user, UUID jobId, Instant generatedAt) throws InterruptedException {
        sleeper.sleep(buildTime);
        String name = user.displayName();
        return new ReportDocument(
                jobId,
                user.id(),
                generatedAt,
                "Report for " + name,
                "Activity report for " + name + " (" + user.id() + ").");
    }
}
