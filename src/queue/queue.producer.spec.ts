import { Queue } from 'bullmq';
import type { TaskPayload, TaskResult } from './interfaces/task.interface';
import { IDEMPOTENCY_TTL_MS } from './queue.constants';
import { QueueProducer, toDeduplicationId } from './queue.producer';

describe('QueueProducer', () => {
  const payload: TaskPayload = { type: 'email', data: { to: 'a@b.c' } };

  it('gives a retried request the same deduplication id', () => {
    expect(toDeduplicationId('order-42', payload, {})).toBe(
      toDeduplicationId(
        'order-42',
        { type: 'email', data: { to: 'a@b.c' } },
        {},
      ),
    );
  });

  it('gives the same key with a different request its own deduplication id', () => {
    const id = toDeduplicationId('order-42', payload, {});
    expect(
      toDeduplicationId('order-42', { ...payload, data: { to: 'x' } }, {}),
    ).not.toBe(id);
    expect(toDeduplicationId('order-42', payload, { attempts: 1 })).not.toBe(
      id,
    );
    expect(toDeduplicationId('order-43', payload, {})).not.toBe(id);
  });

  it('only deduplicates when an idempotency key is given', async () => {
    const add = jest.fn().mockResolvedValue({ id: '1' });
    const producer = new QueueProducer({ add } as unknown as Queue<
      TaskPayload,
      TaskResult
    >);

    await producer.enqueue(payload, { priority: 1 });
    expect(add).toHaveBeenLastCalledWith('email', payload, {
      priority: 1,
      deduplication: undefined,
    });

    await producer.enqueue(payload, { priority: 1, idempotencyKey: 'k' });
    expect(add).toHaveBeenLastCalledWith('email', payload, {
      priority: 1,
      deduplication: {
        id: toDeduplicationId('k', payload, { priority: 1 }),
        ttl: IDEMPOTENCY_TTL_MS,
      },
    });
  });
});
