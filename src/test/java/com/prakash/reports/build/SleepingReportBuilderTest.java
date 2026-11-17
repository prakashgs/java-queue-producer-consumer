package com.prakash.reports.build;

import com.prakash.reports.ReportDocument;
import com.prakash.reports.user.UserProfile;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SleepingReportBuilderTest {
    @Test
    void usesInjectedSleeperInsteadOfWallClock() throws Exception {
        AtomicReference<Duration> slept = new AtomicReference<>();
        Sleeper sleeper = slept::set;
        SleepingReportBuilder builder = new SleepingReportBuilder(Duration.ofSeconds(2), sleeper);
        Instant now = Instant.parse("2026-11-17T00:00:00Z");
        UUID jobId = UUID.randomUUID();

        ReportDocument document = builder.build(new UserProfile("user-1", "Ada"), jobId, now);

        assertEquals(Duration.ofSeconds(2), slept.get());
        assertEquals(jobId, document.jobId());
        assertEquals("user-1", document.userId());
        assertEquals(now, document.generatedAt());
        assertTrue(document.title().contains("Ada"));
        assertTrue(document.body().contains("user-1"));
    }
}
