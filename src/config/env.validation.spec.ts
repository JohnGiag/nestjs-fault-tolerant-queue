import { ConfigModule } from '@nestjs/config';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { validateEnv } from './env.validation';

describe('validateEnv', () => {
  const REDIS_URL = 'redis://localhost:6379';

  // ConfigModule copies only validateEnv's return value into process.env, and parseCreateTask
  // reads NODE_ENV from there: the README's quick start (NODE_ENV in .env) depends on this path.
  it('gets NODE_ENV from a .env file into process.env', async () => {
    const envFile = join(mkdtempSync(join(tmpdir(), 'env-')), '.env');
    writeFileSync(envFile, `REDIS_URL=${REDIS_URL}\nNODE_ENV=development\n`);
    const saved = process.env.NODE_ENV;
    delete process.env.NODE_ENV;
    try {
      await ConfigModule.forRoot({
        envFilePath: envFile,
        validate: validateEnv,
      });
      expect(process.env.NODE_ENV).toBe('development');
    } finally {
      process.env.NODE_ENV = saved;
    }
  });

  it('applies defaults when only REDIS_URL is set', () => {
    expect(validateEnv({ REDIS_URL })).toEqual({
      REDIS_URL,
      PORT: 3000,
      WORKER_CONCURRENCY: 5,
      JOB_ATTEMPTS: 3,
      JOB_BACKOFF_MS: 1000,
      TRUST_PROXY_HOPS: '0',
    });
  });

  it('treats empty values as unset', () => {
    expect(validateEnv({ REDIS_URL, JOB_ATTEMPTS: '' }).JOB_ATTEMPTS).toBe(3);
  });

  it('parses numbers and accepts rediss://', () => {
    const env = validateEnv({
      REDIS_URL: 'rediss://user:pw@host:6380/2',
      JOB_ATTEMPTS: '7',
      TRUST_PROXY_HOPS: '1',
    });
    expect(env.JOB_ATTEMPTS).toBe(7);
    expect(env.TRUST_PROXY_HOPS).toBe('1');
  });

  it.each([undefined, '', 'localhost:6379', 'http://localhost:6379'])(
    'rejects REDIS_URL=%p',
    (url) => {
      expect(() => validateEnv({ REDIS_URL: url })).toThrow(/REDIS_URL/);
    },
  );

  it.each(['0', '-1', '1.5', 'abc'])('rejects JOB_ATTEMPTS=%p', (value) => {
    expect(() => validateEnv({ REDIS_URL, JOB_ATTEMPTS: value })).toThrow(
      /JOB_ATTEMPTS/,
    );
  });
});
