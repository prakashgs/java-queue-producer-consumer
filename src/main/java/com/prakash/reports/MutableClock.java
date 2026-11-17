package com.prakash.reports;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/** Test clock. Production code uses {@link Clock#systemUTC()}. */
public final class MutableClock extends Clock {
    private final ZoneId zone;
    private Instant instant;

    public MutableClock(Instant instant) {
        this(instant, ZoneOffset.UTC);
    }

    private MutableClock(Instant instant, ZoneId zone) {
        this.instant = Objects.requireNonNull(instant, "instant");
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    public static MutableClock at(Instant instant) {
        return new MutableClock(instant);
    }

    public synchronized void advance(Duration duration) {
        instant = instant.plus(Objects.requireNonNull(duration, "duration"));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant(), zone);
    }

    @Override
    public synchronized Instant instant() {
        return instant;
    }
}
