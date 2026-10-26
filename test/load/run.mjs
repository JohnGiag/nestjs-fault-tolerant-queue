// Load test runner (npm run test:load): restarts the stack trusting one proxy hop, clears it,
// runs k6 (test/load/ingest.js), samples Redis and the queue every 2 s, then reports.
// Env: RATE (default 500), DURATION_S (default 60). Wipes the stack's queues and keys.
import { execFileSync, spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';

const url = process.env.LOAD_REDIS_URL ?? 'redis://localhost:6379';
const env = { ...process.env, TRUST_PROXY_HOPS: '1' };
const docker = (...args) =>
  execFileSync('docker', args, { encoding: 'utf8', env }).trim();
const redis = new Redis(url);
const queue = new Queue('default-jobs', { connection: { url } });

const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1);
const usedMemory = async () =>
  Number(/used_memory:(\d+)/.exec(await redis.info('memory'))[1]);
const backlog = async () => {
  const c = await queue.getJobCounts(
    'wait',
    'active',
    'delayed',
    'prioritized',
  );
  return Object.values(c).reduce((a, b) => a + b, 0);
};
// Jobs ever created, minus the ones not finished yet. The completed list itself is trimmed to 1,000.
const processed = async () =>
  Number((await redis.get('bull:default-jobs:id')) ?? 0) - (await backlog());
// In chunks: one DEL with 180,000 keys as arguments overflows the call stack.
async function deleteKeys(keys) {
  for (let i = 0; i < keys.length; i += 10_000)
    await redis.del(...keys.slice(i, i + 10_000));
}
const appMemory = () =>
  docker('stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}')
    .split('\n')
    .filter((l) => l.includes('-app-'))
    .map((l) => l.split(' ')[1]);

try {
  // Recreates the app containers if they ran without TRUST_PROXY_HOPS=1.
  docker('compose', 'up', '-d', '--wait');
  await queue.obliterate({ force: true });
  await redis.del('bull:default-jobs:id');
  for (const pattern of ['rl:*', 'bull:default-jobs:de:*'])
    await deleteKeys(await redis.keys(pattern));
  const baseline = await usedMemory();
  const appBefore = appMemory();

  const samples = [];
  const k6 = spawn(
    'docker',
    ['compose', '--profile', 'load', 'run', '--rm', 'k6'],
    {
      env,
      stdio: 'inherit',
    },
  );
  const k6Done = new Promise((resolve) => k6.on('exit', resolve));
  const start = Date.now();
  let running = true;
  void k6Done.then(() => (running = false));
  while (running) {
    samples.push({
      t: (Date.now() - start) / 1000,
      processed: await processed(),
      backlog: await backlog(),
      memory: await usedMemory(),
    });
    await sleep(2000);
  }
  const k6Exit = await k6Done;

  // Let the workers finish, then split memory by deleting each kind of key in turn:
  // idempotency keys (24 h TTL), rate-limit counters (expire 60 s after a client's last
  // request), and what is left: the queue itself.
  while ((await backlog()) > 0) await sleep(500);
  const appAfter = appMemory();
  const afterRun = await usedMemory();
  const dedupKeys = await redis.keys('bull:default-jobs:de:*');
  await deleteKeys(dedupKeys);
  const withoutDedup = await usedMemory();
  const rateKeys = await redis.keys('rl:*');
  await deleteKeys(rateKeys);
  const queueOnly = await usedMemory();
  const perKey = (bytes, n) => (n ? Math.round(bytes / n) : 0);

  const rates = samples
    .slice(1)
    .map((s, i) => (s.processed - samples[i].processed) / (s.t - samples[i].t));
  const steady = rates.slice(2, -2); // skip ramp-up and wind-down
  const peakBacklog = Math.max(...samples.map((s) => s.backlog));
  const memorySeries = samples
    .filter((_, i) => i % 5 === 0)
    .map((s) => `${Math.round(s.t)}s ${mb(s.memory)}`)
    .join(', ');

  console.log(`
Workers: ${steady.length ? Math.round(Math.min(...steady)) : '?'}-${steady.length ? Math.round(Math.max(...steady)) : '?'} jobs/s processed (steady state), peak backlog ${peakBacklog} jobs
Redis used_memory (MB): baseline ${mb(baseline)}, during run: ${memorySeries}
Redis after run: ${mb(afterRun)} MB =
  ${dedupKeys.length} idempotency keys: ${mb(afterRun - withoutDedup)} MB (${perKey(afterRun - withoutDedup, dedupKeys.length)} bytes each, expire after 24 h)
  ${rateKeys.length} rate-limit counters: ${mb(withoutDedup - queueOnly)} MB (${perKey(withoutDedup - queueOnly, rateKeys.length)} bytes each, expire 60 s after a client's last request)
  queue itself: ${mb(queueOnly)} MB (1,000 retained completed jobs + queue structures)
App replicas memory: before ${appBefore.join(', ')}; after ${appAfter.join(', ')}
k6 exit code: ${k6Exit}`);
  process.exitCode = k6Exit;
} finally {
  await queue.close();
  await redis.quit();
}
