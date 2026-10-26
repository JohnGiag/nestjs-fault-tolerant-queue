import { ConfigService } from '@nestjs/config';
import { Job, Queue, UnrecoverableError } from 'bullmq';
import type { Env } from '../config/env.validation';
import type {
  DeadLetterRecord,
  TaskPayload,
  TaskResult,
} from './interfaces/task.interface';
import { QueueWorker } from './queue.worker';

type TestJob = Job<TaskPayload, TaskResult>;

function makeJob(overrides: Partial<TestJob> = {}): TestJob {
  return {
    id: 'job-1',
    name: 'email',
    queueName: 'default-jobs',
    data: { type: 'email', data: { to: 'a@b.c' } },
    opts: { attempts: 3 },
    attemptsMade: 3,
    timestamp: 1_000,
    finishedOn: 2_000,
    stacktrace: ['s1', 's2', 's3'],
    remove: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as TestJob;
}

describe('QueueWorker', () => {
  let dlqAdd: jest.Mock;
  let getFailed: jest.Mock;
  let worker: QueueWorker;

  beforeEach(() => {
    dlqAdd = jest.fn().mockResolvedValue({ id: 'dlq-1' });
    getFailed = jest.fn().mockResolvedValue([]);
    worker = new QueueWorker(
      {} as ConfigService<Env, true>,
      { getFailed } as unknown as Queue<TaskPayload, TaskResult>,
      {
        add: dlqAdd,
        count: jest.fn().mockResolvedValue(1),
      } as unknown as Queue<DeadLetterRecord>,
    );
  });

  describe('process', () => {
    it('fails the first failTimes attempts, then succeeds', async () => {
      const data: TaskPayload = {
        type: 't',
        data: {},
        simulate: { failTimes: 2 },
      };
      await expect(
        worker.process(makeJob({ data, attemptsMade: 0 })),
      ).rejects.toThrow('attempt 1');
      await expect(
        worker.process(makeJob({ data, attemptsMade: 1 })),
      ).rejects.toThrow('attempt 2');
      await expect(
        worker.process(makeJob({ data, attemptsMade: 2 })),
      ).resolves.toMatchObject({
        type: 't',
      });
    });

    it('throws UnrecoverableError for poison payloads', async () => {
      const data: TaskPayload = {
        type: 't',
        data: {},
        simulate: { poison: true },
      };
      await expect(worker.process(makeJob({ data }))).rejects.toBeInstanceOf(
        UnrecoverableError,
      );
    });
  });

  describe('onFailed', () => {
    it('does not dead-letter an attempt BullMQ will retry', async () => {
      const job = makeJob({ finishedOn: undefined, attemptsMade: 1 });
      await worker.onFailed(job, new Error('boom'));
      expect(dlqAdd).not.toHaveBeenCalled();
      expect(job.remove).not.toHaveBeenCalled();
    });

    it('copies a terminally failed job to the DLQ, then removes it', async () => {
      const job = makeJob({
        opts: { attempts: 3, delay: 5_000, priority: 2 },
      } as Partial<TestJob>);
      const error = new Error('boom');
      await worker.onFailed(job, error);

      expect(dlqAdd).toHaveBeenCalledWith(
        'email',
        {
          originalJobId: 'job-1',
          originalQueue: 'default-jobs',
          jobName: 'email',
          payload: job.data,
          failedReason: 'boom',
          stacktrace: error.stack,
          attemptStacktraces: ['s1', 's2', 's3'],
          attemptsMade: 3,
          maxAttempts: 3,
          jobOptions: { attempts: 3, delay: 5_000, priority: 2 },
          enqueuedAt: new Date(1_000).toISOString(),
          failedAt: new Date(2_000).toISOString(),
        },
        // Same id for the same failure, so a second copy (sweep, other replica) is ignored.
        { jobId: 'job-1-2000' },
      );
      expect(job.remove).toHaveBeenCalled();
      expect(dlqAdd.mock.invocationCallOrder[0]).toBeLessThan(
        (job.remove as jest.Mock).mock.invocationCallOrder[0],
      );
    });

    it('keeps the job in the primary queue when the DLQ write fails', async () => {
      dlqAdd.mockRejectedValue(new Error('redis down'));
      const job = makeJob();
      await expect(
        worker.onFailed(job, new Error('boom')),
      ).resolves.toBeUndefined();
      expect(job.remove).not.toHaveBeenCalled();
    });
  });

  describe('deadLetterLeftovers', () => {
    it('dead-letters jobs left in the failed set, with their stored error', async () => {
      const job = makeJob({
        failedReason: 'connect ETIMEDOUT',
        stacktrace: ['s1', 'Error: connect ETIMEDOUT\n    at x'],
      });
      getFailed.mockResolvedValue([job]);

      await worker.deadLetterLeftovers();

      expect(dlqAdd).toHaveBeenCalledWith(
        'email',
        expect.objectContaining({
          failedReason: 'connect ETIMEDOUT',
          stacktrace: 'Error: connect ETIMEDOUT\n    at x',
        }),
        { jobId: 'job-1-2000' },
      );
      expect(job.remove).toHaveBeenCalled();
    });
  });

  describe('onApplicationBootstrap', () => {
    it('applies WORKER_CONCURRENCY and starts the failed-set sweep', async () => {
      const job = makeJob();
      getFailed.mockResolvedValue([job]);
      const booted = new QueueWorker(
        { get: () => 7 } as unknown as ConfigService<Env, true>,
        { getFailed } as unknown as Queue<TaskPayload, TaskResult>,
        {
          add: dlqAdd,
          count: jest.fn().mockResolvedValue(1),
        } as unknown as Queue<DeadLetterRecord>,
      );
      // WorkerHost exposes the BullMQ worker that @nestjs/bullmq assigns to _worker.
      const fakeWorker = { concurrency: 1 };
      Object.assign(booted, { _worker: fakeWorker });

      booted.onApplicationBootstrap();
      // The sweep runs in the background; every mock resolves at once, so one macrotask is enough.
      await new Promise((resolve) => setImmediate(resolve));

      expect(fakeWorker.concurrency).toBe(7);
      expect(dlqAdd).toHaveBeenCalledWith('email', expect.any(Object), {
        jobId: 'job-1-2000',
      });
      expect(job.remove).toHaveBeenCalled();
    });
  });
});
