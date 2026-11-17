package com.prakash.reports;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportPipelineConfigTest {
    @Test
    void defaultsMatchTheCapacityContract() {
        ReportPipelineConfig config = ReportPipelineConfig.defaults();

        assertEquals(1000, config.incomingCapacity());
        assertTrue(config.workerCount() >= 4);
        assertEquals(4, config.workerCount());
        assertEquals(3, config.maxAttempts());
        assertEquals(8, config.maxInFlightPerUser());
        assertEquals(Duration.ofMillis(25), config.offerTimeout());
        assertEquals(Duration.ofSeconds(30), config.lease());
    }
}
