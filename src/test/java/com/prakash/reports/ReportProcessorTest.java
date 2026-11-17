package com.prakash.reports;

import com.prakash.reports.build.ReportBuilder;
import com.prakash.reports.user.InMemoryUserDirectory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class ReportProcessorTest {
    @Test
    void duplicateDeliveryBuildsOnce() throws Exception {
        AtomicInteger builds = new AtomicInteger();
        CountDownLatch inBuild = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReportBuilder builder = (user, jobId, now) -> {
            builds.incrementAndGet();
            inBuild.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return new ReportDocument(jobId, user.id(), now, "t", "once");
        };
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(4, 1, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                builder,
                MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"))
        )) {
            SubmitResult submitted = pipeline.service().submit("user-1", null);
            UUID id = submitted.job().id();
            assertEquals(id, pipeline.queue().poll(Duration.ZERO));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> first = executor.submit(() -> pipeline.processor().process(id));
                assertTrue(inBuild.await(2, TimeUnit.SECONDS));

                pipeline.processor().process(id);

                assertEquals(1, builds.get());
                release.countDown();
                first.get(2, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                executor.shutdownNow();
            }
            pipeline.processor().process(id);
            assertEquals(1, builds.get());
            assertEquals(JobStatus.SUCCEEDED, pipeline.store().find(id).orElseThrow().status());
            assertEquals("once", pipeline.store().findDocument(id).orElseThrow().body());
        }
    }

    @Test
    void retryThenSuccessKeepsTheInFlightSlot() throws Exception {
        AtomicInteger builds = new AtomicInteger();
        ReportBuilder builder = (user, jobId, now) -> {
            if (builds.incrementAndGet() == 1) {
                throw new IllegalStateException("transient");
            }
            return new ReportDocument(jobId, user.id(), now, "t", "ok");
        };
        try (ReportPipeline pipeline = pipeline(builder, 3, 8)) {
            UUID id = takeSubmitted(pipeline, "user-1");

            pipeline.processor().process(id);

            ReportJob requeued = pipeline.store().find(id).orElseThrow();
            assertEquals(JobStatus.QUEUED, requeued.status());
            assertEquals(1, requeued.attempt());
            assertNull(requeued.claimToken());
            assertTrue(requeued.error().contains("transient"));
            assertEquals(1, pipeline.store().inFlight("user-1"));
            assertEquals(id, pipeline.queue().poll(Duration.ZERO));

            pipeline.processor().process(id);

            ReportJob done = pipeline.store().find(id).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, done.status());
            assertEquals(2, done.attempt());
            assertEquals("ok", pipeline.store().findDocument(id).orElseThrow().body());
            assertEquals(0, pipeline.store().inFlight("user-1"));
            assertEquals(0, pipeline.queue().retryDepth());
        }
    }

    @Test
    void attemptsExhaustedThenFailed() throws Exception {
        ReportBuilder builder = (user, jobId, now) -> {
            throw new IllegalStateException("nope");
        };
        try (ReportPipeline pipeline = pipeline(builder, 3, 1)) {
            UUID id = takeSubmitted(pipeline, "user-1");

            for (int attempt = 1; attempt <= 2; attempt++) {
                pipeline.processor().process(id);
                ReportJob job = pipeline.store().find(id).orElseThrow();
                assertEquals(JobStatus.QUEUED, job.status());
                assertEquals(attempt, job.attempt());
                assertEquals(1, pipeline.store().inFlight("user-1"));
                assertEquals(id, pipeline.queue().poll(Duration.ZERO));
            }

            pipeline.processor().process(id);

            ReportJob failed = pipeline.store().find(id).orElseThrow();
            assertEquals(JobStatus.FAILED, failed.status());
            assertEquals(3, failed.attempt());
            assertTrue(failed.error().contains("nope"));
            assertTrue(pipeline.store().findDocument(id).isEmpty());
            assertEquals(0, pipeline.store().inFlight("user-1"));
            assertNull(pipeline.queue().poll(Duration.ofMillis(10)));

            SubmitResult again = pipeline.service().submit("user-1", null);
            assertEquals(SubmitResult.Status.CREATED, again.status());
        }
    }

    @Test
    void missingUserAtBuildTimeFailsTheJob() throws Exception {
        InMemoryUserDirectory users = TestSupport.users("user-1");
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(4, 1, 1, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                users,
                TestSupport.instantBuilder(),
                MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"))
        )) {
            SubmitResult submitted = pipeline.service().submit("user-1", "gone");
            users.remove("user-1");
            assertEquals(submitted.job().id(), pipeline.queue().poll(Duration.ZERO));

            pipeline.processor().process(submitted.job().id());

            ReportJob failed = pipeline.store().find(submitted.job().id()).orElseThrow();
            assertEquals(JobStatus.FAILED, failed.status());
            assertTrue(failed.error().contains("unknown user"));
            assertEquals(0, pipeline.store().inFlight("user-1"));
        }
    }

    @Test
    void leaseExpiryRequeuesAndInvalidatesStaleClaim() throws Exception {
        MutableClock clock = MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"));
        Duration lease = Duration.ofSeconds(30);
        AtomicInteger builds = new AtomicInteger();
        AtomicReference<UUID> staleToken = new AtomicReference<>();
        AtomicReference<ReportPipeline> pipelineRef = new AtomicReference<>();
        CountDownLatch inBuild = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReportBuilder builder = (user, jobId, now) -> {
            int n = builds.incrementAndGet();
            if (n == 1) {
                staleToken.set(pipelineRef.get().store().find(jobId).orElseThrow().claimToken());
                inBuild.countDown();
                assertTrue(release.await(3, TimeUnit.SECONDS));
            }
            return new ReportDocument(jobId, user.id(), now, "t", "body-" + n);
        };
        ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(4, 1, 3, 8, Duration.ofMillis(25), lease),
                TestSupport.users("user-1"),
                builder,
                clock
        );
        pipelineRef.set(pipeline);
        pipeline.start();
        try {
            SubmitResult submitted = pipeline.service().submit("user-1", "lease");
            UUID id = submitted.job().id();
            assertTrue(inBuild.await(2, TimeUnit.SECONDS));
            UUID stale = staleToken.get();
            assertTrue(stale != null);

            clock.advance(lease.plusSeconds(1));
            pipeline.sweepOnce();

            ReportJob requeued = pipeline.store().find(id).orElseThrow();
            assertEquals(JobStatus.QUEUED, requeued.status());
            assertNull(requeued.claimToken());
            assertEquals(1, requeued.attempt());
            assertEquals(1, pipeline.store().inFlight("user-1"));
            ReportDocument forged = new ReportDocument(id, "user-1", clock.instant(), "stale", "nope");
            assertFalse(pipeline.store().complete(id, stale, forged, clock.instant()));
            assertTrue(pipeline.store().findDocument(id).isEmpty());

            release.countDown();
            ReportJob done = TestSupport.awaitStatus(pipeline.store(), id, JobStatus.SUCCEEDED, Duration.ofSeconds(2));
            assertEquals(2, builds.get());
            assertEquals(2, done.attempt());
            assertNotEquals(stale, done.claimToken());
            assertEquals("body-2", pipeline.store().findDocument(id).orElseThrow().body());
        } finally {
            release.countDown();
            pipeline.close();
        }
    }

    @Test
    void expiredLastAttemptFailsInsteadOfRequeueing() throws Exception {
        MutableClock clock = MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"));
        CountDownLatch inBuild = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger builds = new AtomicInteger();
        ReportBuilder builder = (user, jobId, now) -> {
            builds.incrementAndGet();
            inBuild.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return new ReportDocument(jobId, user.id(), now, "t", "late");
        };
        ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(4, 1, 1, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                builder,
                clock
        ).start();
        try {
            SubmitResult submitted = pipeline.service().submit("user-1", null);
            UUID id = submitted.job().id();
            assertTrue(inBuild.await(2, TimeUnit.SECONDS));
            UUID stale = pipeline.store().find(id).orElseThrow().claimToken();

            clock.advance(Duration.ofSeconds(31));
            pipeline.sweepOnce();

            assertEquals(JobStatus.FAILED, pipeline.store().find(id).orElseThrow().status());
            assertEquals(0, pipeline.queue().retryDepth());
            assertEquals(0, pipeline.store().inFlight("user-1"));
            assertFalse(pipeline.store().complete(
                    id,
                    stale,
                    new ReportDocument(id, "user-1", clock.instant(), "stale", "nope"),
                    clock.instant()));

            release.countDown();
            pipeline.close();
            assertEquals(JobStatus.FAILED, pipeline.store().find(id).orElseThrow().status());
            assertTrue(pipeline.store().findDocument(id).isEmpty());
            assertEquals(1, builds.get());
        } finally {
            release.countDown();
            pipeline.close();
        }
    }

    private static ReportPipeline pipeline(ReportBuilder builder, int maxAttempts, int maxInFlight) {
        return ReportPipeline.create(
                TestSupport.config(4, 1, maxAttempts, maxInFlight, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                builder,
                MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"))
        );
    }

    private static UUID takeSubmitted(ReportPipeline pipeline, String userId) throws Exception {
        SubmitResult submitted = pipeline.service().submit(userId, null);
        assertEquals(SubmitResult.Status.CREATED, submitted.status());
        assertEquals(submitted.job().id(), pipeline.queue().poll(Duration.ZERO));
        return submitted.job().id();
    }
}
