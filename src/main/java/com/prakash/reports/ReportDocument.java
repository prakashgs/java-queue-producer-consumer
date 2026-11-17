package com.prakash.reports;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Generated report body, stored separately from the job row.
 * The queue carries only the job id.
 */
public record ReportDocument(
        UUID jobId,
        String userId,
        Instant generatedAt,
        String title,
        String body
) {
    public ReportDocument {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(generatedAt, "generatedAt");
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(body, "body");
    }
}
