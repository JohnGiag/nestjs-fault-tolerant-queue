import { Test, TestingModule } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { Redis } from 'ioredis';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { validateEnv } from '../src/config/env.validation';
import { RateLimiterModule } from '../src/rate-limiter/rate-limiter.module';
import { RateLimiterService } from '../src/rate-limiter/services/rate-limiter.service';
import { REDIS_CLIENT, RedisModule } from '../src/redis/redis.module';
import { assertLocalRedis } from './local-redis';

// Copied from nestjs-redis-rate-limiter, plus the NOSCRIPT fallback case. Needs Redis:
// docker compose up -d redis
describe('RateLimiterService (Redis integration)', () => {
  const REDIS_URL = process.env.E2E_REDIS_URL ?? 'redis://localhost:6379/15';
  let moduleRef: TestingModule;
  let service: RateLimiterService;
  let redis: Redis;
  let key: string;

  beforeAll(async () => {
    assertLocalRedis(REDIS_URL);
    moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({
          isGlobal: true,
          ignoreEnvFile: true,
          validate: () => validateEnv({ REDIS_URL }),
        }),
        RedisModule,
        RateLimiterModule,
      ],
    }).compile();
    await moduleRef.init();
    service = moduleRef.get(RateLimiterService);
    redis = moduleRef.get<Redis>(REDIS_CLIENT);
  });

  beforeEach(() => {
    key = `rl:test:${randomUUID()}`;
  });

  afterEach(async () => {
    await redis.del(key);
  });

  afterAll(async () => {
    await moduleRef.close();
  });

  it('counts down remaining and reports when a slot frees up', async () => {
    for (const remaining of [2, 1, 0]) {
      await expect(service.consume(key, 3, 10)).resolves.toMatchObject({
        allowed: true,
        remaining,
      });
    }
    const denied = await service.consume(key, 3, 10);
    expect(denied.allowed).toBe(false);
    expect(denied.remaining).toBe(0);
    expect(denied.resetSec).toBeGreaterThanOrEqual(1);
    expect(denied.resetSec).toBeLessThanOrEqual(10);
  });

  it('slides: a slot frees when the oldest request leaves the window', async () => {
    // limit 2 per 1s. A fixed window would reset both slots at t=1000; a sliding one frees only A's.
    expect((await service.consume(key, 2, 1)).allowed).toBe(true); // A, t≈0
    await sleep(500);
    expect((await service.consume(key, 2, 1)).allowed).toBe(true); // B, t≈500
    expect((await service.consume(key, 2, 1)).allowed).toBe(false); // t≈500, full
    await sleep(600);
    expect((await service.consume(key, 2, 1)).allowed).toBe(true); // t≈1100, A expired
    expect((await service.consume(key, 2, 1)).allowed).toBe(false); // B still in window
  });

  it('does not record denied requests, so memory stays at most `limit`', async () => {
    for (let i = 0; i < 10; i++) await service.consume(key, 2, 10);
    expect(await redis.zcard(key)).toBe(2);
  });

  it('never reports negative remaining after a limit is lowered', async () => {
    for (let i = 0; i < 5; i++) await service.consume(key, 5, 10);
    await expect(service.consume(key, 3, 10)).resolves.toMatchObject({
      allowed: false,
      remaining: 0,
    });
  });

  it('sets an expiry so idle clients leave no keys behind', async () => {
    await service.consume(key, 1, 1);
    const ttl = await redis.pttl(key);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(1000);
  });

  it('accepts fractional windows (1.1s is not a float-exact 1100ms)', async () => {
    await expect(service.consume(key, 1, 1.1)).resolves.toMatchObject({
      allowed: true,
    });
  });

  it('keeps working after Redis forgets the script (restart, failover, SCRIPT FLUSH)', async () => {
    expect((await service.consume(key, 2, 10)).allowed).toBe(true);
    await redis.script('FLUSH');
    // EVALSHA now answers NOSCRIPT; the service must fall back to EVAL and keep counting.
    await expect(service.consume(key, 2, 10)).resolves.toMatchObject({
      allowed: true,
      remaining: 0,
    });
    expect((await service.consume(key, 2, 10)).allowed).toBe(false);
  });
});
