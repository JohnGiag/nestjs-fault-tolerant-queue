# NestJS Fault-Tolerant Queue

[![CI](https://github.com/JohnGiag/nestjs-fault-tolerant-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/JohnGiag/nestjs-fault-tolerant-queue/actions/workflows/ci.yml)

A background job service for NestJS, built on BullMQ and Redis. It takes tasks over HTTP, runs them on workers that can be scaled out, retries failures with exponential backoff, and moves jobs that can never succeed to a dead-letter queue (DLQ) with everything needed to diagnose and replay them.

```bash
curl -X POST localhost:3000/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"type":"send-invoice","data":{"orderId":42}}'
# 202 {"jobId":"1"}
```

## Features

- **Retries with exponential backoff and jitter:** a failed job is retried after up to 1 s, then up to 2 s, and so on (configurable), so a struggling downstream system gets breathing room. Each wait is randomised to 50–100% of that value, so jobs that failed together during an outage don't all retry in the same instant.
- **Dead-letter queue:** a job that runs out of attempts, or throws an `UnrecoverableError` (a "poison" job), is copied to `default-jobs-dlq` with its payload, every attempt's stack trace, the attempt counts and timestamps. Only then is it removed from the main queue.
- **No silent loss:** removal always happens after the DLQ copy, never before. If the copy fails (a Redis blip, or a job failing during shutdown after the DLQ connection has closed), the job stays in the main queue's failed list, and every app instance re-routes those leftovers when it starts. Each failure has a fixed DLQ job ID, so a repeated copy is ignored instead of duplicated.
- **Survives crashed workers:** if a worker process dies mid-job, BullMQ hands the job to another worker once its lock expires. The [crash test](#multi-replica-and-crash-test) kills a replica mid-job on every CI run and checks that nothing is lost or duplicated.
- **Measured under load:** on one laptop, 3 replicas accepted 1,500 requests/s for 3 minutes with p99 latency of 3 ms and no errors. The workers kept up, and Redis and app memory stayed flat (see [Performance](#performance)).
- **Graceful shutdown:** on `SIGTERM`, workers finish their active jobs before the process exits.
- **Idempotent ingestion:** a repeated request with the same `Idempotency-Key` returns the original job ID for 24 hours instead of creating a second job, without keeping finished jobs in Redis.
- **Rate-limited at the edge:** `POST /jobs` is limited per client IP with an exact sliding window in Redis, so a flood is refused with `429` before it becomes jobs. Reused from [nestjs-redis-rate-limiter](https://github.com/JohnGiag/nestjs-redis-rate-limiter).
- **Bounded Redis memory:** Redis must run with `noeviction` for BullMQ, so a full Redis stops everything. Payloads are capped at 64 KB, only the last 1,000 finished jobs are kept, and idempotency uses small expiring keys.
- **Fail-fast configuration:** a missing `REDIS_URL` or a malformed number stops the app at startup.

## How it works

```
POST /jobs
  │ 1. RateLimiterGuard        100 req / 60 s per client IP     → 429 (503 if Redis is down)
  │ 2. parseCreateTask         validate body, cap sizes          → 400 / 413
  │ 3. QueueProducer.enqueue   Idempotency-Key → deduplication   → 202 { jobId }
  ▼
Redis: default-jobs ──► QueueWorker (every replica, WORKER_CONCURRENCY jobs each)
                          │
                          ├─ success ───────────────────────► completed (last 1,000 kept)
                          ├─ error, attempts left ──────────► delayed (exponential backoff) ─┐
                          │                                                                   │
                          │◄──────────────────────────────────────────────────────────────────┘
                          └─ error, attempts exhausted
                             or UnrecoverableError ─────────► 'failed' event
                                                                 1. copy to default-jobs-dlq
                                                                 2. remove from default-jobs
```

### Dead-letter routing

BullMQ emits the worker's `failed` event after **every** failed attempt, including ones it will retry. [`QueueWorker.onFailed`](src/queue/queue.worker.ts) tells them apart with `job.finishedOn`, which BullMQ sets only when it gives up on a job. That covers three cases:

| Cause | Example |
|---|---|
| Attempts exhausted | a downstream API kept timing out |
| `UnrecoverableError` thrown | the payload can never succeed (poison pill), so retrying wastes worker time |
| Stalled too often | the job's process crashed twice while running it. BullMQ 6 fails it with an `UnrecoverableError` when a worker next picks it up |

A DLQ entry ([`DeadLetterRecord`](src/queue/interfaces/task.interface.ts)) looks like this:

```json
{
  "originalJobId": "17",
  "originalQueue": "default-jobs",
  "jobName": "send-invoice",
  "payload": { "type": "send-invoice", "data": { "orderId": 42 } },
  "failedReason": "connect ETIMEDOUT",
  "stacktrace": "Error: connect ETIMEDOUT\n    at ...",
  "attemptStacktraces": ["Error: ...", "Error: ...", "Error: ..."],
  "attemptsMade": 3,
  "maxAttempts": 3,
  "jobOptions": { "attempts": 3 },
  "enqueuedAt": "2026-10-26T09:00:00.000Z",
  "failedAt": "2026-10-26T09:00:07.214Z"
}
```

No worker consumes the DLQ. Entries wait there until someone inspects and replays or deletes them (see [Operating the DLQ](#operating-the-dlq)).

### Delivery guarantee

Processing is **at least once**. A job whose worker is killed mid-run is run again from the start by another worker, so **task handlers must be idempotent** (safe to run twice). The outcome is exactly once: every job ends either completed or in the DLQ, never both and never neither. The crash test checks this. A job whose DLQ copy failed waits in the main queue's failed list until the next app start moves it.

### Concurrency

Each app instance runs the HTTP API and one BullMQ worker. A worker runs up to `WORKER_CONCURRENCY` jobs at the same time on one Node.js process: this is async concurrency, not threads, so it suits I/O-bound tasks. Instances compete for jobs from the same Redis queue, so total capacity is `replicas × WORKER_CONCURRENCY`. BullMQ's locks guarantee that a job is held by one worker at a time.

## Quick start

### Option A: Docker

Runs Redis and 3 app replicas.

```bash
docker compose up -d --build --wait
docker compose ps              # each replica's API is on its own random port, on 127.0.0.1 only
npm run test:chaos             # optional: the crash test (wipes the stack's queues)
docker compose down
```

### Option B: Local development

**Requirements:** Node.js 24+ and Docker (for Redis), or Redis 6.2+ reachable from the host.

```bash
docker compose up -d redis     # Redis only, on localhost:6379
npm install
cp .env.example .env
npm run start:dev
```

Try the retry and DLQ paths. The `simulate` field is accepted because `.env.example` sets `NODE_ENV=development`:

```bash
# fails twice, then succeeds on the 3rd attempt
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"flaky","simulate":{"failTimes":2}}'

# poison: goes straight to the DLQ, no retries
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"broken","simulate":{"poison":true}}'
```

## API

### `POST /jobs`

| Header | Required | Description |
|---|---|---|
| `Idempotency-Key` | no | 1–255 characters. See [Idempotency](#idempotency). |

Body:

| Field | Type | Rules |
|---|---|---|
| `type` | string | **Required.** 1–100 characters: letters, digits, `_`, `-`, `.`. Used as the BullMQ job name. |
| `data` | object | Task input. At most 64 KB as JSON. Defaults to `{}`. |
| `options.attempts` | integer | 1–10. Overrides `JOB_ATTEMPTS` for this job. |
| `options.delay` | integer | 0–86,400,000 ms. Wait before the first run. |
| `options.priority` | integer | 0–2,097,152. Lower runs first; 0 means no priority. |
| `simulate.failTimes` | integer | 0–10. Fail the first N attempts. **Development and test only.** |
| `simulate.durationMs` | integer | 0–60,000. Simulated work time. **Development and test only.** |
| `simulate.poison` | boolean | Throw an `UnrecoverableError`. **Development and test only.** |

Responses:

| Status | When |
|---|---|
| `202 Accepted` | Queued (or matched an earlier request with the same `Idempotency-Key`). Body: `{"jobId": "…"}` |
| `400 Bad Request` | Invalid body or header, or `simulate` sent when `NODE_ENV` is not `development` or `test` |
| `413 Payload Too Large` | `data` over 64 KB, or the whole body over Express's 100 KB limit |
| `429 Too Many Requests` | Over 100 requests in 60 s from this client IP. Carries `Retry-After`. |
| `503 Service Unavailable` | Redis unreachable. The rate limiter fails closed rather than letting unmetered traffic through. |

Responses that reached the rate-limit check carry `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`. The `503` and Express's own `413` (rejected while parsing the body, before guards run) don't.

### Idempotency

When a client sends an `Idempotency-Key`, the producer hashes the key **together with the payload and options** into a BullMQ deduplication ID with a 24-hour TTL ([`queue.producer.ts`](src/queue/queue.producer.ts)):

- **Same key, same request, within 24 h:** no new job; the original `jobId` is returned. This holds even after the original job has finished and been trimmed from Redis.
- **Same key, different request:** a separate job. Keys aren't scoped per client (there is no auth), so two clients that happen to pick the same key must not collapse into one job.
- **After the job is dead-lettered:** the key is freed, so resending the request creates a fresh job.

Only a small key per request is stored, not the finished job, so memory doesn't grow with traffic.

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `REDIS_URL` | **yes** | none | `redis[s]://[[user]:password@]host:port[/db]`. The app refuses to start without it. |
| `PORT` | no | `3000` | HTTP port. |
| `NODE_ENV` | no | none | `development` or `test` enables the `simulate` field. Any other value, or none, rejects it. The Docker image sets `production`. |
| `TRUST_PROXY_HOPS` | no | `0` | Number of proxies or load balancers in front of the app. Set it correctly, or every client behind a balancer shares one rate limit (too low) or clients can spoof their IP (too high). |
| `WORKER_CONCURRENCY` | no | `5` | Jobs each instance runs at the same time. |
| `JOB_ATTEMPTS` | no | `3` | Total attempts per job, including the first. |
| `JOB_BACKOFF_MS` | no | `1000` | Base delay of the exponential backoff: retry *n* waits a random 50–100% of `JOB_BACKOFF_MS × 2^(n-1)`. |

Values come from the environment first, then from `.env`. Every number is checked at startup ([`env.validation.ts`](src/config/env.validation.ts)).

**Redis must run with `maxmemory-policy noeviction`** (as in [`compose.yaml`](compose.yaml)). If Redis evicts a job's keys under memory pressure, the queue is corrupted.

## Operating the DLQ

Watch the log line written for every dead letter. It includes the DLQ's current size, which is the number to alert on:

```
ERROR [QueueWorker] Job 17 dead-lettered after 3 attempt(s), DLQ size 4: connect ETIMEDOUT
```

There is no admin endpoint yet. To inspect and replay entries, use BullMQ directly, for example from a one-off script:

```ts
import { Queue } from 'bullmq';

const connection = { url: process.env.REDIS_URL };
const dlq = new Queue('default-jobs-dlq', { connection });
// defaultJobOptions belong to each Queue object, not to Redis: copy the app's,
// or replayed jobs get a single attempt and no backoff.
const queue = new Queue('default-jobs', {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000, jitter: 0.5 },
    removeOnComplete: { count: 1000 },
    removeOnFail: false,
  },
});

for (const entry of await dlq.getWaiting(0, -1)) {
  console.log(entry.data.originalJobId, entry.data.failedReason);
  // Replay once the cause is fixed. jobOptions restores the job's own attempts, delay and priority.
  await queue.add(entry.data.jobName, entry.data.payload, entry.data.jobOptions);
  await entry.remove();
}
```

**Two gotchas:**

- **Every producer sets its own job options.** Attempts and backoff are configured on the app's `Queue` object. Any other service that adds jobs to `default-jobs` must set them as well, or its jobs get one attempt and go straight to the DLQ.
- **Don't `obliterate` the main queue in production.** `queue.obliterate()` keeps the `bull:default-jobs:de:*` deduplication keys, and job IDs restart at 1 afterwards. A repeated request within 24 hours could then get back the ID of a different, newer job. If you must wipe the queue, delete those keys too.

## Performance

Measured with `npm run test:load` ([`test/load/`](test/load/)). [k6](https://k6.io) sends a constant rate of `POST /jobs` requests, spread round-robin over the 3 replicas, while a runner samples Redis and the queue every 2 seconds.

**Setup.** Everything ran on one Windows laptop under Docker Desktop (WSL 2, 16 CPUs, 7.6 GB for Docker): k6, 3 app replicas with 5 worker slots each, and Redis, all sharing the same CPUs. A dedicated server will do better, and a real task handler will be slower than the simulated one, which does no work. Treat these as relative numbers and as a check that nothing degrades, not as a production capacity figure.

**Clients.** The rate limit allows 100 requests a minute per client IP, so one load generator would mostly measure `429`s. The test runs the stack with `TRUST_PROXY_HOPS=1` and gives each request one of 20,000 client addresses in `X-Forwarded-For`, like many clients behind one proxy. Every request also carries a unique `Idempotency-Key`, the worst case for Redis memory.

### Throughput and latency

| Rate | Duration | Requests | Errors | p95 | p99 | Workers keep up? |
|---|---|---|---|---|---|---|
| 500/s | 60 s | 30,001 | 0 | 1.8 ms | 4.6 ms | yes, at most 1 job waiting |
| **1,500/s** | **180 s** | **270,001** | **0** | **1.8 ms** | **3.0 ms** | **yes, at most 13 waiting** |
| 3,000/s | 60 s | 179,951 | 0 | 4.3 ms | 8.8 ms | yes, at most 35 waiting |
| 5,000/s | 60 s | 299,269 | 0 | 17.6 ms | 27.9 ms | no: the workers peaked at about 3,900 jobs/s, about 85,000 jobs queued up, and all were processed after the test |

- **The API was never the bottleneck.** At 5,000 requests/s every request still got `202`. Above 3,000/s, k6 itself couldn't send a few requests on time (50 of 180,000 at 3,000/s, logged as "dropped"). Those never reached the API, so they don't count as errors.
- **The workers are the ceiling,** at about 3,000 jobs/s with 3 replicas × 5 slots for a job that does no work. To go further, add replicas or raise `WORKER_CONCURRENCY`. When the API takes jobs faster than the workers finish them, the queue absorbs the difference and the backlog drains once traffic drops. That is the queue doing its job.

### Memory

Redis memory after each run, split by deleting each kind of key in turn:

| Part | Size | Lifetime |
|---|---|---|
| The queue itself (1,000 kept finished jobs and BullMQ's structures) | **6–7 MB at every rate** | constant |
| Idempotency keys | **about 156 bytes per keyed request** | 24 hours |
| Rate-limit counters | 180–770 bytes per active client, depending on its request rate | 60 seconds after a client's last request |

After the 5,000/s run, Redis held 21,000 keys and 24 MB. 60 seconds later the rate-limit counters had expired and it was back to 6.4 MB.

**Plan Redis memory around idempotency keys.** They are the only part that grows with traffic. Budget about **13.5 MB of Redis per 1 request/s of keyed traffic sustained over 24 hours** (156 bytes × 86,400 s). For example, 100 keyed requests/s needs about 1.3 GB, and 1,500/s needs about 20 GB. Requests without an `Idempotency-Key` cost nothing here. To trade memory for a shorter retry window, lower `IDEMPOTENCY_TTL_MS` in [`queue.constants.ts`](src/queue/queue.constants.ts).

**App memory** started at 50–75 MB per replica and settled at 165–215 MB under load. It then stayed flat: during the 3-minute run at 1,500/s it held at about 165 MB from start to end, and again a minute after the load stopped. There's no sign of a leak.

### Running it

```bash
docker compose up -d --build --wait
npm run test:load                       # 500 requests/s for 60 s
RATE=1500 DURATION_S=180 npm run test:load
```

The runner restarts the app replicas with `TRUST_PROXY_HOPS=1`, wipes the stack's queues, idempotency keys and rate-limit counters, and fails if any k6 threshold is missed: an error, p95 over 50 ms, p99 over 200 ms, or a dropped request. Run `docker compose up -d` afterwards to go back to `TRUST_PROXY_HOPS=0`.

## Testing

```bash
npm test               # unit tests, no Redis needed
npm run test:e2e       # integration tests, need Redis on localhost:6379 (uses db 15 and wipes it)
npm run test:chaos     # multi-replica and crash test, needs the Docker stack running
npm run test:load      # load test with k6, needs the Docker stack running (see Performance)
```

| Layer | What it proves | Needs |
|---|---|---|
| Unit | Request validation and limits, env parsing, idempotency hashing, DLQ routing decisions and write order, startup sweep and its wiring, trust proxy, rate-limit guard and decorator | nothing |
| Integration | Against real BullMQ: retries, exhausted and poison jobs reach the DLQ with stack traces and leave the main queue, flaky jobs complete, idempotency survives trimming. Rate limiter: sliding-window counting, bounded memory, key expiry, and recovery after Redis loses its script cache | Redis |
| Multi-replica and crash test | Through 3 replicas: one `Idempotency-Key` makes one job, and the rate limit is shared. Then every job ends exactly once after a replica is killed mid-job and another is stopped | Docker stack |

### Multi-replica and crash test

[`test/chaos.mjs`](test/chaos.mjs) runs against the compose stack with 3 replicas × 5 worker slots. It wipes the stack's queues and rate-limit counters, so it can be rerun at any time.

**Through the API of all 3 replicas:**

1. **Idempotency:** sends one `Idempotency-Key` to all 3 replicas at the same moment and expects a single job ID. Deduplication is one atomic Redis script, so separate processes can't race past it.
2. **Shared rate limit:** sends 103 requests round-robin over the 3 replicas and expects exactly 100 accepted and 3 refused with `429`. If each replica counted on its own, all 103 would pass, up to 300.

**Crash test,** after the API's jobs have finished and the queues are wiped:

1. Enqueues 60 jobs straight into Redis: 45 normal (3 s each), 10 that fail once and then succeed, and 5 poison jobs.
2. After 1.5 s, while every replica is mid-job, `docker kill`s one replica (`SIGKILL`, no cleanup) and `docker stop`s another (`SIGTERM`, graceful).
3. Waits for the queue to drain. The killed replica's jobs keep their lock for 30 s, then the stall check (every 30 s) hands them to the surviving worker.
4. Checks that every job has exactly one outcome, only the poison jobs are in the DLQ, the failed list is empty, and at least one job was really recovered from the killed replica.
5. Restarts both containers.

```
idempotency: 3 replicas, 1 job (61)
rate limit: 103 requests over 3 replicas, 100 accepted, 3 limited
killed 585a99261d95 (SIGKILL)
stopped b98fbd06ce41 (SIGTERM, graceful)
OK: 55 completed (5 recovered after the kill), 5 dead-lettered, 0 lost, 0 duplicated
```

In every recorded run, exactly 5 jobs were recovered: one replica's worth of slots. That fits the killed replica's jobs being recovered while the gracefully stopped replica finished its own. A run takes about 45 seconds, most of it the 30-second lock.

## Continuous integration

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push and pull request:

1. **test:** `npm ci`, lint, `npm audit --omit=dev --audit-level=high`, build, unit and integration tests, against a Redis service container.
2. **chaos:** builds the Docker stack and runs the multi-replica and crash test, then a 30-second load test at 200 requests/s. The load test is a regression check, not a benchmark: CI runners are smaller and noisier than the machine behind the [Performance](#performance) numbers. It still fails on any error, a dropped request, p95 over 50 ms or p99 over 200 ms.

`package.json` pins `multer` (used inside `@nestjs/platform-express`) to `^2.5.0` through npm `overrides`. That fixes a high-severity denial-of-service advisory; the app has no upload routes, but the audit gate stays clean.

## Docker

| File | What it does |
|---|---|
| [`Dockerfile`](Dockerfile) | Multi-stage build that ships only `dist/` and production dependencies, runs as the non-root `node` user with `NODE_ENV=production`. The health check accepts any HTTP response and doesn't touch Redis, so a Redis outage doesn't get containers restarted. Node runs as PID 1 in exec form and receives `SIGTERM` directly. |
| [`compose.yaml`](compose.yaml) | Redis (`noeviction`, append-only file on the named volume `redis-data`, `127.0.0.1:6379`) and 3 app replicas, each on a random port. Every port is bound to `127.0.0.1` only: this Redis has no password and the API has no auth, and on Linux Docker's published ports bypass the host firewall. Queued jobs and the DLQ survive `docker compose down`; `down -v` wipes them. `stop_grace_period: 30s` gives workers time to finish active jobs. `TRUST_PROXY_HOPS` defaults to `0`; the load test sets `1`. A k6 service under the `load` profile only starts for `npm run test:load`. |

The stack has no load balancer: the queue is the shared state, and workers need none. When you put one in front of the API, set `TRUST_PROXY_HOPS` to the number of proxies.

## Project structure

```
src/
├── main.ts                        # bootstrap, trust proxy, shutdown hooks
├── app.module.ts                  # ConfigModule (validated), RedisModule, TasksModule
├── trust-proxy.ts                 # TRUST_PROXY_HOPS → Express "trust proxy"
├── config/env.validation.ts       # fail-fast environment parsing
├── redis/redis.module.ts          # fail-fast ioredis client for the rate limiter
├── queue/
│   ├── queue.module.ts            # BullMQ connection, default-jobs (retries, backoff, retention), DLQ
│   ├── queue.producer.ts          # enqueue + idempotency (deduplication)
│   ├── queue.worker.ts            # simulated task handler, concurrency, DLQ routing
│   ├── queue.constants.ts         # queue names, idempotency TTL
│   └── interfaces/task.interface.ts  # TaskPayload, TaskJobOptions, DeadLetterRecord
├── tasks/
│   ├── tasks.controller.ts        # POST /jobs, @RateLimit
│   └── create-task.dto.ts         # request validation and caps
└── rate-limiter/                  # sliding-window limiter, from nestjs-redis-rate-limiter
test/
├── dead-letter.e2e-spec.ts        # queue integration tests against real Redis
├── rate-limiter.service.e2e-spec.ts  # sliding-window limiter against real Redis
├── local-redis.ts                 # refuses to run the suites against a non-local Redis
├── chaos.mjs                      # multi-replica and crash test against the Docker stack
└── load/
    ├── ingest.js                  # k6 scenario: constant rate of POST /jobs over the replicas
    └── run.mjs                    # runs k6, samples Redis and the queue, reports throughput and memory
```

## Design decisions

| Decision | Why |
|---|---|
| DLQ routing on the worker's `failed` event, keyed on `job.finishedOn` | The event fires for every attempt; `finishedOn` is set only on the final failure. The integration test pins this behaviour, so a BullMQ upgrade that changes it fails CI. |
| Copy to the DLQ, then remove | The other order loses the job if the DLQ write fails. A crash between the two steps leaves the job in the failed list instead. |
| Startup sweep of the failed list, with a fixed DLQ job ID (`<jobId>-<finishedOn>`) | On shutdown, NestJS closes the DLQ queue in parallel with the worker's drain, so a job failing in that window can't be copied. The sweep catches it, and every other missed copy, on the next start. The fixed ID makes replicas sweeping at the same time, or a sweep after a half-finished copy, harmless. |
| DLQ is a BullMQ queue, not a database table | It shares Redis, BullMQ tooling and job inspection with the main queue, with no extra infrastructure. |
| Two Redis connection setups | BullMQ workers block on Redis and must wait out an outage (`maxRetriesPerRequest: null`). The rate limiter must answer within milliseconds (`maxRetriesPerRequest: 1`, `commandTimeout: 500`). |
| Idempotency through BullMQ deduplication, not the job ID | Using the key as the job ID only works while the job exists, so every finished job had to be kept for 24 h. One busy client could then fill a `noeviction` Redis. |
| Payload and options inside the deduplication hash | There is no auth to scope keys per client. Otherwise one client's key could silently swallow another client's different task. |
| `simulate` allowed only in `development` and `test` | It makes workers sleep and fail on purpose. Allowing it unless `NODE_ENV=production` would expose it whenever a deploy forgets the variable. |
| `removeOnComplete: { count: 1000 }`, not an age | Memory stays constant however much traffic arrives. |
| Worker concurrency set in `onApplicationBootstrap` | `@Processor` options are evaluated at import time, before `ConfigModule` has loaded `.env`. |
| Rate limiter fails closed | An outage must not turn the ingestion endpoint into an unmetered path into Redis. |

## Limitations

- **No authentication.** Rate limits are per IP and idempotency keys are global. Add an auth guard before exposing the API publicly; the rate limiter then counts per user automatically (`req.user.id`).
- **No DLQ tooling.** Inspection and replay are manual (see [Operating the DLQ](#operating-the-dlq)), and the DLQ has no size cap.
- **The task handler is a simulation.** [`QueueWorker.process`](src/queue/queue.worker.ts) is where real task types plug in.
- **Recovery from a crashed worker takes 30–60 s,** BullMQ's default lock duration plus stall-check interval. Shorter values detect crashes faster but risk treating slow, healthy jobs as stalled.
- **Idempotency keys cost Redis memory for 24 hours,** about 156 bytes per keyed request. That is about 20 GB at a sustained 1,500 keyed requests/s; see [Performance](#performance).
- **One Redis.** There is no Sentinel or Cluster setup here. Redis is the single point of failure for both the queue and the rate limiter.

## Tech stack

NestJS · TypeScript (strict) · BullMQ · ioredis · Redis · @nestjs/config · Jest · Docker · GitHub Actions

## License

[MIT](LICENSE) © 2026 Giannis Giagkoudis
