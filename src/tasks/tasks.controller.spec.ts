import { BadRequestException } from '@nestjs/common';
import { GUARDS_METADATA } from '@nestjs/common/constants';
import { QueueProducer } from '../queue/queue.producer';
import {
  RATE_LIMIT_KEY,
  RateLimiterGuard,
} from '../rate-limiter/guards/rate-limiter.guard';
import { TasksController } from './tasks.controller';

describe('TasksController', () => {
  let enqueue: jest.Mock;
  let controller: TasksController;

  beforeEach(() => {
    enqueue = jest.fn().mockResolvedValue('1');
    controller = new TasksController({ enqueue } as unknown as QueueProducer);
  });

  // The guard is the only thing between a flood and Redis: removing it must fail a test.
  it('rate-limits POST /jobs', () => {
    const handler = Object.getOwnPropertyDescriptor(
      TasksController.prototype,
      'create',
    )!.value as object;
    expect(Reflect.getMetadata(GUARDS_METADATA, handler)).toContain(
      RateLimiterGuard,
    );
    expect(Reflect.getMetadata(RATE_LIMIT_KEY, handler)).toEqual({
      limit: 100,
      ttlSec: 60,
    });
  });

  it('enqueues the parsed task with its idempotency key', async () => {
    await expect(
      controller.create({ type: 'email', options: { attempts: 2 } }, 'k'),
    ).resolves.toEqual({ jobId: '1' });
    expect(enqueue).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'email' }),
      expect.objectContaining({ attempts: 2, idempotencyKey: 'k' }),
    );
  });

  it.each(['', 'k'.repeat(256)])(
    'rejects Idempotency-Key of length %#',
    async (key) => {
      await expect(controller.create({ type: 'email' }, key)).rejects.toThrow(
        BadRequestException,
      );
      expect(enqueue).not.toHaveBeenCalled();
    },
  );
});
