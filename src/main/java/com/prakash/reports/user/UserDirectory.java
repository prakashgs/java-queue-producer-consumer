package com.prakash.reports.user;

/**
 * Cheap existence check on the request thread, profile load on the worker.
 * A production directory would be a user service; this interface is the seam.
 */
public interface UserDirectory {
    boolean exists(String userId);

    UserProfile load(String userId);
}
