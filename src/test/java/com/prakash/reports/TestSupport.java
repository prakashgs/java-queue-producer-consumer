package com.prakash.reports;

import com.prakash.reports.build.ReportBuilder;
import com.prakash.reports.store.ReportStore;
import com.prakash.reports.user.InMemoryUserDirectory;
import com.prakash.reports.user.UserProfile;

import java.time.Duration;
import java.util.UUID;

public final class TestSupport {
    private TestSupport() {
    }

    public static ReportPipelineConfig config(
            int capacity,
            int workers,
            int maxAttempts,
            int maxInFlight,
            Duration offerTimeout,
            Duration lease
    ) {
        return new ReportPipelineConfig(
                capacity,
                workers,
                maxAttempts,
                maxInFlight,
                offerTimeout,
                lease,
                Duration.ofMillis(20),
                Duration.ZERO);
    }

    public static InMemoryUserDirectory users(String... ids) {
        InMemoryUserDirectory directory = new InMemoryUserDirectory();
        for (String id : ids) {
            directory.add(new UserProfile(id, id));
        }
        return directory;
    }

    public static ReportBuilder instantBuilder() {
        return (user, jobId, now) -> new ReportDocument(
                jobId,
                user.id(),
                now,
                "Report for " + user.displayName(),
                "body:" + user.id());
    }

    public static ReportJob awaitStatus(ReportStore store, UUID id, JobStatus status, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        ReportJob last = null;
        while (System.nanoTime() < deadline) {
            last = store.find(id).orElse(null);
            if (last != null && last.status() == status) {
                return last;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timed out waiting for " + status + " last=" + last);
    }
}
