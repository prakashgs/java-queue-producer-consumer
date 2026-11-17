package com.prakash.reports;

import com.prakash.reports.build.SleepingReportBuilder;
import com.prakash.reports.build.Sleeper;
import com.prakash.reports.http.ReportHttpServer;
import com.prakash.reports.user.InMemoryUserDirectory;
import com.prakash.reports.user.UserProfile;

import java.time.Clock;
import java.time.Duration;

/**
 * Local demo. Seeds {@code user-1} and listens on port 8080.
 * Each report build sleeps for 2 seconds on a worker, not on the request thread.
 */
public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        InMemoryUserDirectory users = new InMemoryUserDirectory();
        users.add(new UserProfile("user-1", "Ada Lovelace"));
        ReportPipelineConfig config = ReportPipelineConfig.defaults();
        ReportPipeline pipeline = ReportPipeline.create(
                config,
                users,
                new SleepingReportBuilder(Duration.ofSeconds(2), Sleeper.thread()),
                Clock.systemUTC()
        ).start();
        ReportHttpServer server = ReportHttpServer.start(8080, pipeline.service());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            pipeline.close();
        }, "report-shutdown"));
        System.out.println("Report server listening on http://127.0.0.1:" + server.port());
        System.out.println("Seeded userId=user-1");
        System.out.printf(
                "Throughput is about %.0f reports/sec while each build takes 2s (%d workers).%n",
                config.workerCount() / 2.0,
                config.workerCount());
        try {
            Thread.currentThread().join();
        } finally {
            server.close();
            pipeline.close();
        }
    }
}
