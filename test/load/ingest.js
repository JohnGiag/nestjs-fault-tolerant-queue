// k6 load test: constant request rate into POST /jobs, spread over the 3 replicas.
// Started by test/load/run.mjs (npm run test:load), which also samples Redis and the queue.
// Env: RATE requests/s (default 500), DURATION_S (default 60), CLIENTS (default 20000).
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';

const RATE = Number(__ENV.RATE || 500);
const DURATION_S = Number(__ENV.DURATION_S || 60);
// Many clients behind one proxy (TRUST_PROXY_HOPS=1 for this run): each address stays far under
// the 100 req/min limit (20,000 clients: under 100/min each up to ~30,000 req/s), so the test
// measures ingestion, not 429s.
const CLIENTS = Number(__ENV.CLIENTS || 20000);

export const options = {
  // Docker's DNS returns every replica's address; resolve on every request and rotate.
  dns: { ttl: '0', select: 'roundRobin' },
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: 100,
      maxVUs: 1000,
    },
  },
  thresholds: {
    http_req_failed: ['rate==0'], // every request answered 202: no 429, 5xx, timeouts
    checks: ['rate==1'],
    http_req_duration: ['p(95)<50', 'p(99)<200'], // milliseconds
    dropped_iterations: ['count==0'], // the API kept up with the requested rate
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const n = exec.scenario.iterationInTest;
  const client = n % CLIENTS;
  const res = http.post(
    'http://app:3000/jobs',
    JSON.stringify({ type: 'load', data: { n } }),
    {
      headers: {
        'content-type': 'application/json',
        'x-forwarded-for': `10.0.${client >> 8}.${client & 255}`,
        // Every request carries a unique key: worst case for idempotency-key memory.
        'idempotency-key': `load-${n}`,
      },
    },
  );
  check(res, { 'status is 202': (r) => r.status === 202 });
}
