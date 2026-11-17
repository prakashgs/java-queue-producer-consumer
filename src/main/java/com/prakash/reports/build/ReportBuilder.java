package com.prakash.reports.build;

import com.prakash.reports.ReportDocument;
import com.prakash.reports.user.UserProfile;

import java.time.Instant;
import java.util.UUID;

@FunctionalInterface
public interface ReportBuilder {
    ReportDocument build(UserProfile user, UUID jobId, Instant generatedAt) throws Exception;
}
