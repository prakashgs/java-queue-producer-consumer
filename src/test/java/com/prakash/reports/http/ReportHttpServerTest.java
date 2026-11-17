package com.prakash.reports.http;

import com.prakash.reports.ReportPipeline;
import com.prakash.reports.ReportPipelineConfig;
import com.prakash.reports.TestSupport;
import com.prakash.reports.user.InMemoryUserDirectory;
import com.prakash.reports.user.UserProfile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(10)
class ReportHttpServerTest {
    @Test
    void jsonRoundTripOnEphemeralPort() throws Exception {
        try (Fixture fixture = Fixture.start()) {
            HttpResult created = post(fixture.port, "application/json",
                    "{\"userId\":\"user-1\",\"idempotencyKey\":\"json-key\"}");

            assertEquals(202, created.status);
            UUID id = jobId(created.body);
            HttpResult fetched = pollUntilSucceeded(fixture.port, id);
            assertEquals(200, fetched.status);
            assertTrue(fetched.body.contains("\"status\":\"SUCCEEDED\""));
            assertTrue(fetched.body.contains("Report for Ada"));
            assertTrue(fetched.body.contains("\"document\""));
        }
    }

    @Test
    void formRoundTripReplaysTheSameJob() throws Exception {
        try (Fixture fixture = Fixture.start()) {
            HttpResult created = post(fixture.port, "application/x-www-form-urlencoded",
                    "userId=user-1&idempotencyKey=form-key");
            assertEquals(202, created.status);
            UUID id = jobId(created.body);

            HttpResult replay = post(fixture.port, "application/x-www-form-urlencoded",
                    "userId=user-1&idempotencyKey=form-key");
            assertEquals(200, replay.status);
            assertEquals(id, jobId(replay.body));

            HttpResult fetched = pollUntilSucceeded(fixture.port, id);
            assertTrue(fetched.body.contains("\"status\":\"SUCCEEDED\""));
            assertTrue(fetched.body.contains("Activity report") || fetched.body.contains("body:"));
        }
    }

    @Test
    void unknownUserAndUnknownJob() throws Exception {
        try (Fixture fixture = Fixture.start()) {
            HttpResult missingUser = post(fixture.port, "application/json", "{\"userId\":\"nobody\"}");
            assertEquals(404, missingUser.status);
            assertTrue(missingUser.body.contains("unknown_user"));

            HttpResult missingJob = get(fixture.port, UUID.randomUUID());
            assertEquals(404, missingJob.status);
            assertTrue(missingJob.body.contains("not_found"));
        }
    }

    private static HttpResult pollUntilSucceeded(int port, UUID id) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        HttpResult last = null;
        while (System.nanoTime() < deadline) {
            last = get(port, id);
            if (last.status == 200 && last.body.contains("\"status\":\"SUCCEEDED\"")) {
                return last;
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for the report: " + last);
        return last;
    }

    private static UUID jobId(String body) {
        Matcher matcher = Pattern.compile("\"id\":\"([^\"]+)\"").matcher(body);
        assertTrue(matcher.find(), body);
        return UUID.fromString(matcher.group(1));
    }

    private static HttpResult post(int port, String contentType, String body) throws IOException {
        return exchange("POST", "http://127.0.0.1:" + port + "/reports", contentType, body);
    }

    private static HttpResult get(int port, UUID id) throws IOException {
        return exchange("GET", "http://127.0.0.1:" + port + "/reports/" + id, null, null);
    }

    private static HttpResult exchange(String method, String url, String contentType, String body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        connection.setConnectTimeout(1000);
        connection.setReadTimeout(1000);
        connection.setRequestMethod(method);
        if (body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", contentType);
            try (OutputStream out = connection.getOutputStream()) {
                out.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int status = connection.getResponseCode();
        InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        String response = "";
        if (stream != null) {
            try (InputStream in = stream) {
                response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        connection.disconnect();
        return new HttpResult(status, response);
    }

    private record HttpResult(int status, String body) {
    }

    private static final class Fixture implements AutoCloseable {
        private final ReportPipeline pipeline;
        private final ReportHttpServer server;
        private final int port;

        private Fixture(ReportPipeline pipeline, ReportHttpServer server) {
            this.pipeline = pipeline;
            this.server = server;
            this.port = server.port();
        }

        static Fixture start() throws Exception {
            InMemoryUserDirectory users = new InMemoryUserDirectory();
            users.add(new UserProfile("user-1", "Ada"));
            ReportPipelineConfig config = TestSupport.config(20, 2, 3, 8, Duration.ofMillis(25), Duration.ofSeconds(30));
            ReportPipeline pipeline = ReportPipeline.create(config, users, TestSupport.instantBuilder(), Clock.systemUTC()).start();
            ReportHttpServer server = ReportHttpServer.start(0, pipeline.service());
            return new Fixture(pipeline, server);
        }

        @Override
        public void close() {
            server.close();
            pipeline.close();
        }
    }
}
