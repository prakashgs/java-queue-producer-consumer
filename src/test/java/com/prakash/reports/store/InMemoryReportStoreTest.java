package com.prakash.reports.store;

import com.prakash.reports.ReportJob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class InMemoryReportStoreTest {
    @Test
    void concurrentCreateAndAbandonDoesNotReturnADeletedJob() throws Exception {
        InMemoryReportStore store = new InMemoryReportStore(100_000);
        Instant now = Instant.parse("2026-11-17T00:00:00Z");
        int threads = 8;
        int iterations = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<UUID> handedOut = new ConcurrentLinkedQueue<>();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            CreateResult result = store.reserve("user-1", "same-key", now);
                            if (result.kind() == CreateResult.Kind.CREATED) {
                                try {
                                    if ((i & 3) == 0) {
                                        store.commit(result.job().id());
                                        handedOut.add(result.job().id());
                                    } else {
                                        store.abandon(result.job().id());
                                    }
                                } catch (RuntimeException | Error failure) {
                                    store.abandon(result.job().id());
                                    throw failure;
                                }
                            } else if (result.kind() == CreateResult.Kind.EXISTING) {
                                handedOut.add(result.job().id());
                            }
                        }
                        return null;
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    } catch (BrokenBarrierException broken) {
                        throw new IllegalStateException(broken);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(8, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertFalse(handedOut.isEmpty());
        for (UUID id : handedOut) {
            ReportJob job = store.find(id).orElse(null);
            assertTrue(job != null, "handed out deleted job " + id);
        }
    }
}
