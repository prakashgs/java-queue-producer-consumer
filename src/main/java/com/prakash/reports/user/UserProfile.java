package com.prakash.reports.user;

import java.util.Objects;

public record UserProfile(String id, String displayName) {
    public UserProfile {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(displayName, "displayName");
        if (id.isBlank()) {
            throw new IllegalArgumentException("id is blank");
        }
    }
}
