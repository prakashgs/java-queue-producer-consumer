package com.prakash.reports.user;

public final class UnknownUserException extends RuntimeException {
    private final String userId;

    public UnknownUserException(String userId) {
        super("unknown user: " + userId);
        this.userId = userId;
    }

    public String userId() {
        return userId;
    }
}
