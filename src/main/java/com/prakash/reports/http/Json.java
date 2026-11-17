package com.prakash.reports.http;

import com.prakash.reports.ReportDocument;
import com.prakash.reports.ReportJob;

import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Json {
    private Json() {
    }

    static String job(ReportJob job, ReportDocument document) {
        StringBuilder json = new StringBuilder(256);
        json.append("{\"id\":\"").append(job.id()).append('"');
        json.append(",\"userId\":").append(string(job.userId()));
        json.append(",\"idempotencyKey\":").append(string(job.idempotencyKey()));
        json.append(",\"status\":\"").append(job.status().name()).append('"');
        json.append(",\"attempt\":").append(job.attempt());
        json.append(",\"error\":").append(string(job.error()));
        if (document == null) {
            json.append(",\"document\":null");
        } else {
            json.append(",\"document\":{");
            json.append("\"jobId\":\"").append(document.jobId()).append('"');
            json.append(",\"userId\":").append(string(document.userId()));
            json.append(",\"generatedAt\":").append(string(DateTimeFormatter.ISO_INSTANT.format(document.generatedAt())));
            json.append(",\"title\":").append(string(document.title()));
            json.append(",\"body\":").append(string(document.body()));
            json.append('}');
        }
        json.append('}');
        return json.toString();
    }

    static String error(String code) {
        return "{\"error\":" + string(code) + "}";
    }

    static String error(String code, String message) {
        return "{\"error\":" + string(code) + ",\"message\":" + string(message) + "}";
    }

    /** Absent fields and JSON null both come back as null. */
    static String stringField(String json, String name) {
        if (json == null || json.isBlank()) {
            return null;
        }
        Matcher stringValue = Pattern.compile(
                "\"" + Pattern.quote(name) + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"",
                Pattern.DOTALL).matcher(json);
        if (stringValue.find()) {
            return unescape(stringValue.group(1));
        }
        return null;
    }

    private static String string(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    private static String unescape(String raw) {
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (i + 1 >= raw.length()) {
                throw new IllegalArgumentException("truncated escape");
            }
            char next = raw.charAt(++i);
            switch (next) {
                case '"', '\\', '/' -> out.append(next);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (i + 4 >= raw.length()) {
                        throw new IllegalArgumentException("bad unicode escape");
                    }
                    int codePoint = Integer.parseInt(raw.substring(i + 1, i + 5), 16);
                    out.append((char) codePoint);
                    i += 4;
                }
                default -> throw new IllegalArgumentException("bad escape");
            }
        }
        return out.toString();
    }
}
