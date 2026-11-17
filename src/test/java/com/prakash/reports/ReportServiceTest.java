package com.prakash.reports;

import com.prakash.reports.build.ReportBuilder;
import com.prakash.reports.queue.JobQueue;
import com.prakash.reports.store.InMemoryReportStore;
import com.prakash.reports.user.InMemoryUserDirectory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(10)
class ReportServiceTest {
    @Test
    void submitReturnsWhileBuildIsBlocked() throws Exception {
        CountDownLatch inBuild = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReportBuilder builder = (user, jobId, now) -> {
            inBuild.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return new ReportDocument(jobId, user.id(), now, "t", "b");
        };
        ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(10, 2, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                builder,
                Clock.systemUTC()
        ).start();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<SubmitResult> submitted = executor.submit(() -> pipeline.service().submit("user-1", "fast"));
            try {
                SubmitResult result = submitted.get(1, TimeUnit.SECONDS);
                assertEquals(SubmitResult.Status.CREATED, result.status());
                assertTrue(inBuild.await(2, TimeUnit.SECONDS));
                assertEquals(JobStatus.IN_PROGRESS, pipeline.store().find(result.job().id()).orElseThrow().status());
            } finally {
                release.countDown();
                submitted.get(2, TimeUnit.SECONDS);
            }
        } finally {
            release.countDown();
            pipeline.close();
            executor.shutdown();
        }
    }

    @Test
    void queueFullAbandonsJobSoTheSameIdempotencyKeyCanCreateANewOne() throws Exception {
        ReportPipelineConfig config = TestSupport.config(1, 1, 3, 8, Duration.ofMillis(40), Duration.ofSeconds(30));
        try (ReportPipeline pipeline = ReportPipeline.create(
                config,
                TestSupport.users("filler", "user-1"),
                TestSupport.instantBuilder(),
                Clock.systemUTC()
        )) {
            SubmitResult filler = pipeline.service().submit("filler", null);
            assertEquals(SubmitResult.Status.CREATED, filler.status());

            SubmitResult rejected = pipeline.service().submit("user-1", "replay");

            assertEquals(SubmitResult.Status.QUEUE_FULL, rejected.status());
            assertNull(rejected.job());
            assertTrue(pipeline.store().findByIdempotency("user-1", "replay").isEmpty());
            assertEquals(0, pipeline.store().inFlight("user-1"));

            assertEquals(filler.job().id(), pipeline.queue().poll(Duration.ZERO));

            SubmitResult accepted = pipeline.service().submit("user-1", "replay");

            assertEquals(SubmitResult.Status.CREATED, accepted.status());
            assertNotEquals(filler.job().id(), accepted.job().id());
            assertEquals(accepted.job().id(), pipeline.store().findByIdempotency("user-1", "replay").orElseThrow().id());
            assertEquals(JobStatus.QUEUED, pipeline.store().find(accepted.job().id()).orElseThrow().status());
            assertEquals(1, pipeline.store().inFlight("user-1"));
        }
    }

    @Test
    void perUserInFlightCapRejectsWithoutEnqueue() throws Exception {
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(10, 1, 3, 2, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1", "user-2"),
                TestSupport.instantBuilder(),
                Clock.systemUTC()
        )) {
            SubmitResult first = pipeline.service().submit("user-1", "a");
            SubmitResult second = pipeline.service().submit("user-1", "b");
            int depth = pipeline.queue().incomingDepth();

            SubmitResult rejected = pipeline.service().submit("user-1", "c");

            assertEquals(SubmitResult.Status.USER_CAP, rejected.status());
            assertNull(rejected.job());
            assertEquals(depth, pipeline.queue().incomingDepth());
            assertEquals(2, pipeline.store().inFlight("user-1"));
            assertTrue(pipeline.store().findByIdempotency("user-1", "c").isEmpty());

            SubmitResult replay = pipeline.service().submit("user-1", "a");
            assertEquals(SubmitResult.Status.EXISTING, replay.status());
            assertEquals(first.job().id(), replay.job().id());
            assertEquals(depth, pipeline.queue().incomingDepth());

            SubmitResult otherUser = pipeline.service().submit("user-2", "a");
            assertEquals(SubmitResult.Status.CREATED, otherUser.status());
            assertNotEquals(first.job().id(), otherUser.job().id());

            pipeline.processor().process(second.job().id());
            assertEquals(1, pipeline.store().inFlight("user-1"));

            SubmitResult afterRelease = pipeline.service().submit("user-1", "d");
            assertEquals(SubmitResult.Status.CREATED, afterRelease.status());
            assertEquals(2, pipeline.store().inFlight("user-1"));
        }
    }

    @Test
    void idempotencyKeyReturnsSameJobAndEnqueuesOnce() throws Exception {
        MutableClock clock = MutableClock.at(Instant.parse("2026-11-17T00:00:00Z"));
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(10, 1, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1", "user-2"),
                TestSupport.instantBuilder(),
                clock
        )) {
            SubmitResult first = pipeline.service().submit("user-1", "daily");
            SubmitResult replay = pipeline.service().submit("user-1", "daily");

            assertEquals(SubmitResult.Status.CREATED, first.status());
            assertEquals(SubmitResult.Status.EXISTING, replay.status());
            assertEquals(first.job().id(), replay.job().id());
            assertEquals(1, pipeline.queue().incomingDepth());

            SubmitResult otherUser = pipeline.service().submit("user-2", "daily");
            assertEquals(SubmitResult.Status.CREATED, otherUser.status());
            assertNotEquals(first.job().id(), otherUser.job().id());
            assertEquals(2, pipeline.queue().incomingDepth());

            SubmitResult blank = pipeline.service().submit("user-1", "  ");
            SubmitResult missing = pipeline.service().submit("user-1", null);
            assertEquals(SubmitResult.Status.CREATED, blank.status());
            assertEquals(SubmitResult.Status.CREATED, missing.status());
            assertNotEquals(blank.job().id(), missing.job().id());
            assertNotEquals(first.job().id(), blank.job().id());

            assertEquals(first.job().id(), pipeline.queue().poll(Duration.ZERO));
            pipeline.processor().process(first.job().id());
            SubmitResult afterSuccess = pipeline.service().submit("user-1", "daily");
            assertEquals(SubmitResult.Status.EXISTING, afterSuccess.status());
            assertEquals(first.job().id(), afterSuccess.job().id());
            assertEquals(JobStatus.SUCCEEDED, afterSuccess.job().status());
            assertEquals(3, pipeline.queue().incomingDepth());
        }
    }

    @Test
    void unknownAndInvalidUsersNeverEnqueue() throws Exception {
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(10, 1, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                TestSupport.instantBuilder(),
                Clock.systemUTC()
        )) {
            assertEquals(SubmitResult.Status.UNKNOWN_USER, pipeline.service().submit("missing", "k").status());
            assertEquals(SubmitResult.Status.INVALID, pipeline.service().submit("  ", "k").status());
            assertEquals(SubmitResult.Status.INVALID, pipeline.service().submit(null, "k").status());
            assertEquals(SubmitResult.Status.INVALID, pipeline.service().submit("u".repeat(129), null).status());
            assertEquals(SubmitResult.Status.INVALID, pipeline.service().submit("user-1", "k".repeat(129)).status());

            assertEquals(0, pipeline.queue().incomingDepth());
            assertEquals(0, pipeline.store().inFlight("missing"));
            assertEquals(0, pipeline.store().inFlight("user-1"));
        }
    }

    @Test
    void interruptedOfferAbandonsTheJob() throws Exception {
        InMemoryReportStore store = new InMemoryReportStore(8);
        JobQueue queue = new JobQueue() {
            @Override
            public boolean offerIncoming(UUID jobId, Duration timeout) throws InterruptedException {
                throw new InterruptedException("offer interrupted");
            }

            @Override
            public void offerRetry(UUID jobId) {
            }

            @Override
            public UUID poll(Duration timeout) {
                return null;
            }

            @Override
            public void stopWaiting() {
            }

            @Override
            public int incomingDepth() {
                return 0;
            }

            @Override
            public int retryDepth() {
                return 0;
            }
        };
        InMemoryUserDirectory users = TestSupport.users("user-1");
        ReportService service = new ReportService(
                store,
                queue,
                users,
                Clock.systemUTC(),
                TestSupport.config(4, 1, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30))
        );
        try {
            assertThrows(InterruptedException.class, () -> service.submit("user-1", "key"));
            assertTrue(store.findByIdempotency("user-1", "key").isEmpty());
            assertEquals(0, store.inFlight("user-1"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void concurrentSubmitsOnAFullQueueNeverReturnADeletedJob() throws Exception {
        try (ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(1, 1, 3, 100, Duration.ofMillis(5), Duration.ofSeconds(30)),
                TestSupport.users("filler", "user-1"),
                TestSupport.instantBuilder(),
                Clock.systemUTC()
        )) {
            assertEquals(SubmitResult.Status.CREATED, pipeline.service().submit("filler", null).status());
            int threads = 8;
            int iterations = 12;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            AtomicInteger surprises = new AtomicInteger();
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    futures.add(executor.submit(() -> {
                        for (int n = 0; n < iterations; n++) {
                            SubmitResult result = pipeline.service().submit("user-1", "same");
                            if (result.status() != SubmitResult.Status.QUEUE_FULL || result.job() != null) {
                                surprises.incrementAndGet();
                            }
                            if (pipeline.store().findByIdempotency("user-1", "same").isPresent()) {
                                surprises.incrementAndGet();
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    future.get(8, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
            }
            assertEquals(0, surprises.get());
            assertEquals(0, pipeline.store().inFlight("user-1"));
            assertEquals(1, pipeline.queue().incomingDepth());
        }
    }

    @Test
    void shutdownWaitsForTheInFlightBuild() throws Exception {
        CountDownLatch inBuild = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReportBuilder builder = (user, jobId, now) -> {
            inBuild.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return new ReportDocument(jobId, user.id(), now, "t", "finished");
        };
        ReportPipeline pipeline = ReportPipeline.create(
                TestSupport.config(4, 1, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30)),
                TestSupport.users("user-1"),
                builder,
                Clock.systemUTC()
        ).start();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SubmitResult submitted = pipeline.service().submit("user-1", null);
            assertTrue(inBuild.await(2, TimeUnit.SECONDS));
            Future<?> closed = executor.submit(pipeline::close);
            try {
                closed.get(250, TimeUnit.MILLISECONDS);
                fail("shutdown returned before the in-flight build finished");
            } catch (TimeoutException expected) {
                release.countDown();
                closed.get(2, TimeUnit.SECONDS);
            }
            ReportJob done = pipeline.store().find(submitted.job().id()).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, done.status());
            assertEquals("finished", pipeline.store().findDocument(done.id()).orElseThrow().body());
        } finally {
            release.countDown();
            pipeline.close();
            executor.shutdown();
        }
    }
}
