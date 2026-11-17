package com.prakash.reports.user;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryUserDirectory implements UserDirectory {
    private final ConcurrentHashMap<String, UserProfile> users = new ConcurrentHashMap<>();

    public void add(UserProfile profile) {
        users.put(Objects.requireNonNull(profile, "profile").id(), profile);
    }

    public void remove(String userId) {
        users.remove(Objects.requireNonNull(userId, "userId"));
    }

    @Override
    public boolean exists(String userId) {
        return userId != null && users.containsKey(userId);
    }

    @Override
    public UserProfile load(String userId) {
        UserProfile profile = userId == null ? null : users.get(userId);
        if (profile == null) {
            throw new UnknownUserException(userId);
        }
        return profile;
    }
}
