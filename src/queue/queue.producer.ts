import { InjectQueue } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { Queue } from 'bullmq';
import { createHash } from 'node:crypto';
import { DEFAULT_QUEUE, IDEMPOTENCY_TTL_MS } from './queue.constants';
import type {
  TaskJobOptions,
  TaskPayload,
  TaskResult,
} from './interfaces/task.interface';

export interface EnqueueOptions extends TaskJobOptions {
  idempotencyKey?: string;
}

@Injectable()
export class QueueProducer {
  constructor(
    @InjectQueue(DEFAULT_QUEUE)
    private readonly queue: Queue<TaskPayload, TaskResult>,
  ) {}

  /**
   * Idempotency via BullMQ deduplication: the first request stores a small Redis key (id → job id)
   * that expires after IDEMPOTENCY_TTL_MS. A repeat within that time is not added and gets the
   * original job id back, even after removeOnComplete has trimmed that job. Unlike using the key
   * as the job id, this does not require keeping every finished job in Redis.
   * An explicit job.remove() does free the key: a dead-lettered job can be submitted again.
   */
  async enqueue(
    payload: TaskPayload,
    { idempotencyKey, ...jobOptions }: EnqueueOptions = {},
  ): Promise<string> {
    const job = await this.queue.add(payload.type, payload, {
      ...jobOptions,
      deduplication: idempotencyKey
        ? {
            id: toDeduplicationId(idempotencyKey, payload, jobOptions),
            ttl: IDEMPOTENCY_TTL_MS,
          }
        : undefined,
    });
    // id is always set once add() resolves; the type is optional only for jobs not yet saved.
    return job.id!;
  }
}

/**
 * The request is part of the id: keys are not scoped per client, so two clients picking the same
 * key for different tasks must not collapse into one job. Hashed to keep the Redis key short.
 */
export function toDeduplicationId(
  idempotencyKey: string,
  payload: TaskPayload,
  jobOptions: TaskJobOptions,
): string {
  const request = JSON.stringify([idempotencyKey, payload, jobOptions]);
  return createHash('sha256').update(request).digest('hex');
}
