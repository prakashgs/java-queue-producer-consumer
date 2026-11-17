package com.prakash.reports.build;

import java.time.Duration;

@FunctionalInterface
public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    static Sleeper thread() {
        return duration -> {
            long millis = duration.toMillis();
            if (millis > 0) {
                Thread.sleep(millis);
            }
        };
    }
}
