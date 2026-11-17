package com.prakash.reports.queue;

import java.time.Duration;
import java.util.UUID;

/**
 * Incoming work is bounded. Retries are a separate unbounded queue so a worker
 * can always hand a job back without waiting on a full incoming queue.
 * The payload is the job id only.
 *
 * <p>A production queue would be SQS or Kafka. This interface is the seam.
 */
public interface JobQueue {
    boolean offerIncoming(UUID jobId, Duration timeout) throws InterruptedException;

    void offerRetry(UUID jobId);

    UUID poll(Duration timeout) throws InterruptedException;

    /** Wake blocked pollers and stop handing out work. Items already taken are still processed. */
    void stopWaiting();

    int incomingDepth();

    int retryDepth();
}
