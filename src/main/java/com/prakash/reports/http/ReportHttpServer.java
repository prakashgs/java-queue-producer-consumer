package com.prakash.reports.http;

import com.prakash.reports.JobStatus;
import com.prakash.reports.ReportDocument;
import com.prakash.reports.ReportJob;
import com.prakash.reports.ReportService;
import com.prakash.reports.SubmitResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** JDK HTTP server for {@code POST /reports} and {@code GET /reports/{id}}. */
public final class ReportHttpServer implements AutoCloseable {
    private static final int MAX_BODY_BYTES = 64 * 1024;

    private final HttpServer server;
    private final ExecutorService executor;

    private ReportHttpServer(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    public static ReportHttpServer start(int port, ReportService service) throws IOException {
        Objects.requireNonNull(service, "service");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        ExecutorService executor = Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable, "report-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/reports", exchange -> new Handler(service).handle(exchange));
        server.start();
        return new ReportHttpServer(server, executor);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private static final class Handler {
        private final ReportService service;

        private Handler(ReportService service) {
            this.service = service;
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                route(exchange);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                send(exchange, 503, Json.error("interrupted"));
            } catch (IllegalArgumentException invalid) {
                String message = invalid.getMessage() == null ? "invalid request" : invalid.getMessage();
                send(exchange, 400, Json.error("invalid_request", message));
            } catch (Exception failure) {
                send(exchange, 500, Json.error("internal"));
            }
        }

        private void route(HttpExchange exchange) throws IOException, InterruptedException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            if ("POST".equals(method) && "/reports".equals(path)) {
                handlePost(exchange);
                return;
            }
            if ("GET".equals(method) && path.startsWith("/reports/")) {
                String rawId = path.substring("/reports/".length());
                if (rawId.isEmpty() || rawId.indexOf('/') >= 0) {
                    send(exchange, 404, Json.error("not_found"));
                    return;
                }
                handleGet(exchange, rawId);
                return;
            }
            send(exchange, 404, Json.error("not_found"));
        }

        private void handlePost(HttpExchange exchange) throws IOException, InterruptedException {
            String body = readBody(exchange);
            String userId;
            String idempotencyKey;
            if (isJson(exchange)) {
                userId = Json.stringField(body, "userId");
                idempotencyKey = Json.stringField(body, "idempotencyKey");
            } else {
                Map<String, String> form = parseForm(body);
                userId = form.get("userId");
                idempotencyKey = form.get("idempotencyKey");
            }
            SubmitResult result = service.submit(userId, idempotencyKey);
            switch (result.status()) {
                case CREATED -> writeJob(exchange, 202, result.job());
                case EXISTING -> writeJob(exchange, 200, result.job());
                case UNKNOWN_USER -> send(exchange, 404, Json.error("unknown_user"));
                case USER_CAP, QUEUE_FULL -> send(exchange, 429, Json.error(result.detail()));
                case INVALID -> send(exchange, 400, Json.error("invalid_request", result.detail()));
            }
        }

        private void handleGet(HttpExchange exchange, String rawId) throws IOException {
            UUID id;
            try {
                id = UUID.fromString(rawId);
            } catch (IllegalArgumentException invalid) {
                send(exchange, 400, Json.error("invalid_id"));
                return;
            }
            Optional<ReportJob> job = service.find(id);
            if (job.isEmpty()) {
                send(exchange, 404, Json.error("not_found"));
                return;
            }
            send(exchange, 200, Json.job(job.get(), documentIfSucceeded(job.get())));
        }

        private void writeJob(HttpExchange exchange, int status, ReportJob snapshot) throws IOException {
            ReportJob job = service.find(snapshot.id()).orElse(snapshot);
            send(exchange, status, Json.job(job, documentIfSucceeded(job)));
        }

        private ReportDocument documentIfSucceeded(ReportJob job) {
            if (job.status() != JobStatus.SUCCEEDED) {
                return null;
            }
            return service.findDocument(job.id()).orElse(null);
        }
    }

    private static boolean isJson(HttpExchange exchange) {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        return contentType != null && contentType.toLowerCase(Locale.ROOT).contains("json");
    }

    private static Map<String, String> parseForm(String body) {
        Map<String, String> fields = new HashMap<>();
        if (body == null || body.isBlank()) {
            return fields;
        }
        for (String part : body.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int separator = part.indexOf('=');
            String rawKey = separator < 0 ? part : part.substring(0, separator);
            String rawValue = separator < 0 ? "" : part.substring(separator + 1);
            fields.put(urlDecode(rawKey), urlDecode(rawValue));
        }
        return fields;
    }

    private static String urlDecode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            byte[] bytes = in.readNBytes(MAX_BODY_BYTES + 1);
            if (bytes.length > MAX_BODY_BYTES) {
                throw new IllegalArgumentException("body exceeds 64KiB");
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static void send(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
