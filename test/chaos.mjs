// Multi-replica test against the compose stack (docker compose up -d --build --wait).
// 1. Through the API of all 3 replicas: one Idempotency-Key sent to every replica at once makes
//    one job, and the rate limit is one budget shared by the replicas, not one each.
// 2. Crash test: kills one replica mid-job (SIGKILL: no cleanup) and stops another (SIGTERM:
//    graceful), then checks the at-least-once guarantee end to end: every job ends exactly once,
//    either completed or in the DLQ, and nothing is lost or left behind.
// Wipes the stack's queues and rate-limit counters.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';

const connection = {
  url: process.env.CHAOS_REDIS_URL ?? 'redis://localhost:6379',
};
// defaultJobOptions live on each Queue instance, not in Redis: mirror the app's (compose env defaults).
const queue = new Queue('default-jobs', {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000, jitter: 0.5 },
    removeOnComplete: { count: 1000 },
    removeOnFail: false,
  },
});
const dlq = new Queue('default-jobs-dlq', { connection });
// For the rate limiter's keys: BullMQ 6 queues no longer expose their Redis client.
const redis = new Redis(connection.url);
const docker = (...args) =>
  execFileSync('docker', args, { encoding: 'utf8' }).trim();

// Jobs go straight into Redis: the API rejects "simulate" in the production container.
const job = (type, simulate) => ({
  name: type,
  data: { type, data: {}, simulate },
});
const plan = [
  ...Array.from({ length: 45 }, () => job('normal', { durationMs: 3000 })),
  ...Array.from({ length: 10 }, () =>
    job('flaky', { durationMs: 1000, failTimes: 1 }),
  ),
  ...Array.from({ length: 5 }, () => job('poison', { poison: true })),
];

async function waitUntilDrained(timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const counts = await queue.getJobCounts(
      'wait',
      'active',
      'delayed',
      'prioritized',
      'paused',
    );
    if (Object.values(counts).every((n) => n === 0)) return;
    await sleep(1000);
  }
  throw new Error(
    `Queue not drained after ${timeoutMs / 1000}s: ${JSON.stringify(await queue.getJobCounts())}`,
  );
}

// Each replica's API, e.g. "127.0.0.1:52951" (random host ports, see compose.yaml).
const apis = [1, 2, 3].map((i) =>
  docker('compose', 'port', '--index', String(i), 'app', '3000'),
);
const postJob = (api, headers = {}) =>
  fetch(`http://${api}/jobs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify({ type: 'replica-check', data: {} }),
  });
// Earlier runs (60 s window) would otherwise have used up part of the limit.
async function resetRateLimits() {
  const keys = await redis.keys('rl:*');
  if (keys.length) await redis.del(...keys);
}

const [killed, stopped] = docker('compose', 'ps', '-q', 'app').split('\n');
assert.ok(killed && stopped, 'needs at least 2 running app replicas');

try {
  // Idempotency across replicas: deduplication is one atomic Redis script, so concurrent
  // requests on different processes must still agree on a single job.
  await resetRateLimits();
  const key = randomUUID();
  const jobIds = await Promise.all(
    apis.map(async (api) => {
      const res = await postJob(api, { 'idempotency-key': key });
      assert.equal(res.status, 202, `${api} answered ${res.status}`);
      return (await res.json()).jobId;
    }),
  );
  assert.equal(
    new Set(jobIds).size,
    1,
    `one key sent to 3 replicas made several jobs: ${jobIds.join(', ')}`,
  );
  console.log(`idempotency: 3 replicas, 1 job (${jobIds[0]})`);

  // Rate limit across replicas: 100 per 60 s per client, counted in Redis, so round-robin over
  // 3 replicas must still stop at 100 (per-replica counting would allow 300).
  await resetRateLimits();
  const statuses = [];
  for (let i = 0; i < 103; i++)
    statuses.push((await postJob(apis[i % 3])).status);
  const accepted = statuses.filter((s) => s === 202).length;
  const limited = statuses.filter((s) => s === 429).length;
  assert.deepEqual(
    { accepted, limited },
    { accepted: 100, limited: 3 },
    `103 requests over 3 replicas: ${accepted} accepted, ${limited} rate-limited`,
  );
  console.log(
    'rate limit: 103 requests over 3 replicas, 100 accepted, 3 limited',
  );
  await resetRateLimits();

  // Let the API's jobs finish, so the crash test below can account for every job it adds.
  await waitUntilDrained(30_000);
  await queue.obliterate({ force: true });
  await dlq.obliterate({ force: true });

  const added = await queue.addBulk(plan);
  const typeOf = new Map(added.map((j) => [j.id, j.name]));

  // 15 worker slots, 3 s jobs: after 1.5 s every replica is mid-job.
  await sleep(1500);
  docker('kill', killed);
  console.log(`killed ${killed.slice(0, 12)} (SIGKILL)`);
  docker('stop', stopped);
  console.log(`stopped ${stopped.slice(0, 12)} (SIGTERM, graceful)`);

  // Jobs of the killed replica keep their lock for 30 s, then the stall check (every 30 s) requeues them.
  await waitUntilDrained(150_000);

  const completed = await queue.getCompleted(0, -1);
  const deadLetters = (await dlq.getWaiting(0, -1)).map(
    (j) => j.data.originalJobId,
  );
  const outcomes = [...completed.map((j) => j.id), ...deadLetters];

  assert.equal(await queue.getFailedCount(), 0, 'jobs left in the failed set');
  assert.equal(
    new Set(outcomes).size,
    outcomes.length,
    'a job has more than one outcome',
  );
  assert.deepEqual(new Set(outcomes), new Set(typeOf.keys()), 'a job was lost');
  for (const id of deadLetters)
    assert.equal(typeOf.get(id), 'poison', `job ${id} dead-lettered`);

  const recovered = completed.filter((j) => j.stalledCounter > 0).length;
  assert.ok(
    recovered > 0,
    'no job was recovered from the killed replica: the kill missed',
  );

  console.log(
    `OK: ${completed.length} completed (${recovered} recovered after the kill), ` +
      `${deadLetters.length} dead-lettered, 0 lost, 0 duplicated`,
  );
} finally {
  // Leave the stack as we found it.
  docker('start', killed, stopped);
  await queue.close();
  await dlq.close();
  await redis.quit();
}
