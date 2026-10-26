import { getQueueToken } from '@nestjs/bullmq';
import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { validateEnv } from '../src/config/env.validation';
import type { DeadLetterRecord } from '../src/queue/interfaces/task.interface';
import { DEFAULT_DLQ, DEFAULT_QUEUE } from '../src/queue/queue.constants';
import { QueueModule } from '../src/queue/queue.module';
import { QueueProducer } from '../src/queue/queue.producer';
import { assertLocalRedis } from './local-redis';

// Runs the real BullMQ retry/fail cycle, so it catches a BullMQ upgrade that changes when
// finishedOn is set (the worker's dead-letter signal). Needs Redis: docker compose up -d redis
describe('dead-letter routing (real Redis)', () => {
  // Real queues on a possibly busy machine (e.g. alongside the compose stack): Jest's 5 s is too tight.
  jest.setTimeout(30_000);

  // db 15: obliterate below must not touch a developer's queues.
  const REDIS_URL = process.env.E2E_REDIS_URL ?? 'redis://localhost:6379/15';

  let moduleRef: TestingModule;
  let queue: Queue;
  let dlq: Queue<DeadLetterRecord>;
  let producer: QueueProducer;

  async function waitFor(check: () => Promise<boolean>): Promise<void> {
    for (let i = 0; i < 250; i++) {
      if (await check()) return;
      await sleep(100);
    }
    throw new Error('timed out after 25s');
  }

  beforeAll(async () => {
    // obliterate wipes both queues: refuse anything but a local Redis.
    assertLocalRedis(REDIS_URL);
    moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({
          isGlobal: true,
          ignoreEnvFile: true,
          validate: () => validateEnv({ REDIS_URL, JOB_BACKOFF_MS: '10' }),
        }),
        QueueModule,
      ],
    }).compile();
    queue = moduleRef.get(getQueueToken(DEFAULT_QUEUE));
    dlq = moduleRef.get(getQueueToken(DEFAULT_DLQ));
    producer = moduleRef.get(QueueProducer);
    await queue.obliterate({ force: true });
    await dlq.obliterate({ force: true });
    await moduleRef.init();
  });

  afterAll(() => moduleRef?.close());

  it('dead-letters exhausted and poison jobs, and lets recovered jobs complete', async () => {
    const doomed = await producer.enqueue(
      { type: 'doomed', data: { n: 1 }, simulate: { failTimes: 5 } },
      { attempts: 2 },
    );
    const poison = await producer.enqueue(
      { type: 'poison', data: {}, simulate: { poison: true } },
      { attempts: 3 },
    );
    const flaky = await producer.enqueue(
      { type: 'flaky', data: {}, simulate: { failTimes: 1 } },
      { attempts: 2 },
    );

    await waitFor(
      async () =>
        (await dlq.count()) === 2 &&
        (await queue.getJobState(flaky)) === 'completed',
    );

    const records = (await dlq.getWaiting()).map((j) => j.data);
    expect(records).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          originalJobId: doomed,
          attemptsMade: 2,
          maxAttempts: 2,
          payload: expect.objectContaining({ data: { n: 1 } }),
        }),
        expect.objectContaining({ originalJobId: poison, attemptsMade: 1 }),
      ]),
    );
    expect(records.every((r) => r.attemptStacktraces.length > 0)).toBe(true);
    // Dead-lettered jobs are removed from the primary queue.
    await waitFor(async () => (await queue.getFailedCount()) === 0);
  });

  it('returns the original job for a repeated Idempotency-Key, even after it is trimmed', async () => {
    const payload = { type: 'idem', data: { order: 42 } };
    // Unique per run: obliterate() keeps deduplication keys, and job ids restart at 1 after it,
    // so a fixed key would dedupe to a previous run's id that now names no job (or another one).
    const key = randomUUID();
    const first = await producer.enqueue(payload, { idempotencyKey: key });
    await waitFor(async () => (await queue.getJobState(first)) === 'completed');
    // Push enough jobs through for removeOnComplete { count: 1000 } to trim the first one.
    // The deduplication key must outlive it (explicit remove() or clean() would delete the key).
    await queue.addBulk(
      Array.from({ length: 1000 }, () => ({
        name: 'filler',
        data: { type: 'filler', data: {} },
      })),
    );
    await waitFor(async () => (await queue.getJob(first)) === undefined);

    expect(await producer.enqueue(payload, { idempotencyKey: key })).toBe(
      first,
    );
    expect(
      await producer.enqueue(
        { ...payload, data: { order: 43 } },
        { idempotencyKey: key },
      ),
    ).not.toBe(first);
  });
});
