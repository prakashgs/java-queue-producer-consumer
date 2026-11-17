package com.prakash.reports.store;

import com.prakash.reports.ReportJob;

public final class CreateResult {
    public enum Kind {
        CREATED,
        EXISTING,
        USER_CAP
    }

    private final Kind kind;
    private final ReportJob job;

    private CreateResult(Kind kind, ReportJob job) {
        this.kind = kind;
        this.job = job;
    }

    public static CreateResult created(ReportJob job) {
        return new CreateResult(Kind.CREATED, job);
    }

    public static CreateResult existing(ReportJob job) {
        return new CreateResult(Kind.EXISTING, job);
    }

    public static CreateResult userCap() {
        return new CreateResult(Kind.USER_CAP, null);
    }

    public Kind kind() {
        return kind;
    }

    public ReportJob job() {
        return job;
    }
}
