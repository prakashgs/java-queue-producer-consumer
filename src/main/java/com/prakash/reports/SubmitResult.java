package com.prakash.reports;

import java.util.Objects;

public record SubmitResult(Status status, ReportJob job, String detail) {
    public enum Status {
        CREATED,
        EXISTING,
        UNKNOWN_USER,
        USER_CAP,
        QUEUE_FULL,
        INVALID
    }

    public SubmitResult {
        Objects.requireNonNull(status, "status");
        if ((status == Status.CREATED || status == Status.EXISTING) && job == null) {
            throw new IllegalArgumentException("accepted submit requires a job");
        }
    }

    public static SubmitResult created(ReportJob job) {
        return new SubmitResult(Status.CREATED, job, null);
    }

    public static SubmitResult existing(ReportJob job) {
        return new SubmitResult(Status.EXISTING, job, null);
    }

    public static SubmitResult unknownUser() {
        return new SubmitResult(Status.UNKNOWN_USER, null, "unknown_user");
    }

    public static SubmitResult userCap() {
        return new SubmitResult(Status.USER_CAP, null, "user_cap");
    }

    public static SubmitResult queueFull() {
        return new SubmitResult(Status.QUEUE_FULL, null, "queue_full");
    }

    public static SubmitResult invalid(String detail) {
        return new SubmitResult(Status.INVALID, null, Objects.requireNonNull(detail, "detail"));
    }
}
