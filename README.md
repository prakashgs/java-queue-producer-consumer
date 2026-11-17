# Report queue

HTTP requests to build a report return as soon as the job is queued. A report takes about two seconds, so the request thread never builds it. Workers claim jobs, build the document, and callers poll for status.

JDK only at runtime (no Spring). Java 17 bytecode. Tests are JUnit 5.

## Flow

`POST /reports` with `userId` and an optional `idempotencyKey`:

1. The request thread checks that the user exists. An unknown user is `404` and nothing is queued.
2. It inserts a `QUEUED` job and reserves one in-flight slot for that user. More than `maxInFlightPerUser` jobs in `QUEUED` or `IN_PROGRESS` is `429` (`user_cap`).
3. It offers **only the job id** onto a bounded incoming queue. The offer waits about 25ms. If the queue is full, the job row is deleted and the response is `429` (`queue_full`), so the same idempotency key can be retried.
4. A new job returns `202` and the job id. A present idempotency key that already belongs to that user returns `200` and the original job.

`GET /reports/{id}` returns the job. Once the status is `SUCCEEDED`, the body includes the document.

Workers claim `QUEUED` to `IN_PROGRESS` with a claim token and a lease, load the user profile, build the report, store the document, then mark `SUCCEEDED`. The document is written before the status flips, and only if the claim token still matches, so a reader never sees `SUCCEEDED` without a document and a stale worker cannot publish over a newer claim.

A failed build is placed on a **separate unbounded retry queue**, not the bounded incoming queue, up to 3 attempts, then `FAILED`. The split matters: a worker must never block while putting a retry onto a full incoming queue, or the pool deadlocks. Delivery is at-least-once. A duplicate delivery builds once, because claim succeeds only from `QUEUED`, and complete/fail require the current claim token.

If a worker dies mid-build, the lease sweeper returns the expired `IN_PROGRESS` job to `QUEUED` and offers it to the retry queue. The old claim token no longer completes or fails the job. If that claim had already used the last attempt, the sweeper marks the job `FAILED` instead, so a build that never returns cannot spin forever.

Shutdown stops taking new work and lets the build already in progress finish. It does not interrupt that build.

A blank or missing idempotency key always creates a new report. A present key is unique per user. The same key returns the original job after success or failure. `userId` rejects blank. `userId` and the idempotency key are capped at 128 characters.

Submit only checks that the user exists. The worker loads the profile. Builds do not change user state, and the same user may have up to `maxInFlightPerUser` jobs running in parallel. If the user is gone by the time the worker runs, that build fails and uses the same attempt budget.

The in-flight slot is reserved with the job and released once, when the job succeeds, fails terminally, or the submit is abandoned. A retry does not release it.

## Defaults and capacity

| Knob | Default |
| --- | --- |
| Incoming queue capacity | 1000 |
| Workers | 4 |
| Attempts | 3 |
| In-flight jobs per user | 8 |
| Offer timeout | 25ms |
| Claim lease | 30s |

Each build holds a worker for 2 seconds, so steady-state throughput is about `workerCount / 2` reports per second. With 4 workers that is about 2 reports per second. A full queue of 1000 is roughly 500 seconds of backlog at that rate. Callers that arrive faster get `429` within the offer timeout instead of waiting out the build. One user can hold at most 8 of those slots, so a single user cannot fill the queue. The retry queue is unbounded on purpose; each job retries at most twice after the first attempt.

## What this process does not do

The in-memory store is **not durable**. Job rows, queue messages, and documents all disappear when the process exits.

Completed documents stay in memory for the life of the process. There is no eviction. A long-running demo will grow with every successful report.

`ReportStore`, `JobQueue`, `UserDirectory`, and `ReportBuilder` are the production seams. A real deployment would put job rows in a database, the queue on SQS or Kafka, and the document in object storage. Those backends are not implemented. The queue message is the UUID only.

There is no authentication. `Main` is a local demo.

## Run

OpenJDK 21 is keg-only on this machine. Maven must run on that JDK. The compiler still targets Java 17.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
mvn test
mvn -q compile
java -cp target/classes com.prakash.reports.Main
```

`Main` seeds `user-1` (Ada Lovelace) and listens on `http://127.0.0.1:8080`.

## Examples

Form body:

```bash
curl -s -D - -X POST http://127.0.0.1:8080/reports \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'userId=user-1&idempotencyKey=daily-2026-11-17'
```

JSON body (any `Content-Type` that contains `json`):

```bash
curl -s -D - -X POST http://127.0.0.1:8080/reports \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-1","idempotencyKey":"daily-2026-11-17"}'
```

A new job is HTTP 202. Repeating the same user and idempotency key is HTTP 200 and the same id. Poll until `SUCCEEDED`:

```bash
curl -s http://127.0.0.1:8080/reports/<job-id>
```

A succeeded body looks like:

```json
{
  "id": "…",
  "userId": "user-1",
  "idempotencyKey": "daily-2026-11-17",
  "status": "SUCCEEDED",
  "attempt": 1,
  "error": null,
  "document": {
    "jobId": "…",
    "userId": "user-1",
    "generatedAt": "2026-11-17T12:00:02Z",
    "title": "Report for Ada Lovelace",
    "body": "Activity report for Ada Lovelace (user-1)."
  }
}
```

`POST` returns the same shape. `202` means the job was accepted. Poll `GET` for the current status. `429` is `{"error":"queue_full"}` or `{"error":"user_cap"}`. An unknown user is `404` `{"error":"unknown_user"}`.
