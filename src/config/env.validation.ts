export interface Env {
  REDIS_URL: string;
  PORT: number;
  WORKER_CONCURRENCY: number;
  JOB_ATTEMPTS: number;
  JOB_BACKOFF_MS: number;
  /** Checked by applyTrustProxy. */
  TRUST_PROXY_HOPS: string;
  /** Read from process.env by parseCreateTask: development or test enables `simulate`. */
  NODE_ENV: string | undefined;
}

function positiveInt(
  raw: Record<string, unknown>,
  name: string,
  fallback: number,
): number {
  const value = raw[name];
  if (value === undefined || value === '') return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1) {
    throw new Error(
      `${name} must be a positive integer, got "${String(value)}"`,
    );
  }
  return n;
}

/** Passed to ConfigModule: a missing or malformed variable stops the app at boot, not at the first job. */
export function validateEnv(raw: Record<string, unknown>): Env {
  const url = raw.REDIS_URL;
  if (typeof url !== 'string' || !/^rediss?:\/\//.test(url)) {
    throw new Error('REDIS_URL is required, e.g. redis://localhost:6379');
  }
  return {
    REDIS_URL: url,
    PORT: positiveInt(raw, 'PORT', 3000),
    WORKER_CONCURRENCY: positiveInt(raw, 'WORKER_CONCURRENCY', 5),
    JOB_ATTEMPTS: positiveInt(raw, 'JOB_ATTEMPTS', 3),
    JOB_BACKOFF_MS: positiveInt(raw, 'JOB_BACKOFF_MS', 1000),
    TRUST_PROXY_HOPS:
      typeof raw.TRUST_PROXY_HOPS === 'string' ? raw.TRUST_PROXY_HOPS : '0',
    // ConfigModule copies only the keys returned here into process.env: without this line,
    // NODE_ENV from .env never reaches process.env.
    NODE_ENV: typeof raw.NODE_ENV === 'string' ? raw.NODE_ENV : undefined,
  };
}
