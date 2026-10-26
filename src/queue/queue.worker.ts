import {
  InjectQueue,
  OnWorkerEvent,
  Processor,
  WorkerHost,
} from '@nestjs/bullmq';
import { Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Job, Queue, UnrecoverableError } from 'bullmq';
import { setTimeout as sleep } from 'node:timers/promises';
import type { Env } from '../config/env.validation';
import { DEFAULT_DLQ, DEFAULT_QUEUE } from './queue.constants';
import type {
  DeadLetterRecord,
  TaskPayload,
  TaskResult,
} from './interfaces/task.interface';

@Processor(DEFAULT_QUEUE)
export class QueueWorker extends WorkerHost implements OnApplicationBootstrap {
  private readonly logger = new Logger(QueueWorker.name);

  constructor(
    private readonly config: ConfigService<Env, true>,
    @InjectQueue(DEFAULT_QUEUE)
    private readonly queue: Queue<TaskPayload, TaskResult>,
    @InjectQueue(DEFAULT_DLQ)
    private readonly dlq: Queue<DeadLetterRecord>,
  ) {
    super();
  }

  /**
   * Concurrency: one worker runs up to N jobs at once on this Node process (async I/O, not threads),
   * and each app replica adds its own N. Set here rather than in @Processor because decorators run
   * at import time, before ConfigModule has loaded .env.
   */
  onApplicationBootstrap(): void {
    this.worker.concurrency = this.config.get('WORKER_CONCURRENCY', {
      infer: true,
    });
    // In the background: the API should not wait for a long failed list to drain.
    void this.deadLetterLeftovers().catch((err: unknown) =>
      this.logger.error(
        `Failed-set sweep aborted: ${err instanceof Error ? err.message : String(err)}`,
      ),
    );
  }

  /**
   * Routes jobs that failed for good but never reached the DLQ: the DLQ write failed (Redis blip),
   * the process died between write and remove, or the job failed during shutdown, when the DLQ
   * queue may already be closed while the worker drains. Safe on every replica at once: the DLQ
   * job id is fixed per failure, so a second copy is ignored.
   */
  async deadLetterLeftovers(): Promise<void> {
    for (const job of await this.queue.getFailed(0, -1)) {
      // The original error object is gone; rebuild it from what BullMQ stored.
      const error = new Error(job.failedReason);
      error.stack = job.stacktrace?.at(-1) ?? error.stack;
      await this.onFailed(job, error);
    }
  }

  /**
   * A thrown error fails this attempt. BullMQ then either schedules a retry with exponential
   * backoff (queue defaultJobOptions) or, once attempts are exhausted, marks the job failed.
   */
  async process(job: Job<TaskPayload, TaskResult>): Promise<TaskResult> {
    const { type, simulate = {} } = job.data;

    if (simulate.poison) {
      // Retrying a payload that can never succeed only burns worker time: fail permanently now.
      throw new UnrecoverableError(`Poison payload for task "${type}"`);
    }
    if (simulate.durationMs) await sleep(simulate.durationMs);
    // attemptsMade counts finished attempts, so it is 0 on the first run.
    if (job.attemptsMade < (simulate.failTimes ?? 0)) {
      throw new Error(
        `Simulated transient failure (attempt ${job.attemptsMade + 1}) for task "${type}"`,
      );
    }
    return { type, processedAt: new Date().toISOString() };
  }

  /**
   * Fires after every failed attempt, including ones BullMQ will retry. BullMQ sets finishedOn only
   * when it gives up (attempts exhausted, UnrecoverableError or maxStartedAttempts), so that is the
   * signal for a dead letter. Jobs that stalled too often (e.g. their process crashed) also end up
   * here: BullMQ 6 fails them with an UnrecoverableError when a worker next picks them up.
   */
  @OnWorkerEvent('failed')
  async onFailed(
    job: Job<TaskPayload, TaskResult> | undefined,
    error: Error,
  ): Promise<void> {
    if (!job?.id) return;
    if (!job.finishedOn) {
      this.logger.warn(
        `Job ${job.id} attempt ${job.attemptsMade} failed, will retry: ${error.message}`,
      );
      return;
    }

    const record: DeadLetterRecord = {
      originalJobId: job.id,
      originalQueue: job.queueName,
      jobName: job.name,
      payload: job.data,
      failedReason: error.message,
      stacktrace: error.stack,
      attemptStacktraces: job.stacktrace ?? [],
      attemptsMade: job.attemptsMade,
      maxAttempts: job.opts.attempts ?? 1,
      jobOptions: {
        attempts: job.opts.attempts,
        delay: job.opts.delay,
        priority: job.opts.priority,
      },
      enqueuedAt: new Date(job.timestamp).toISOString(),
      failedAt: new Date(job.finishedOn).toISOString(),
    };

    try {
      // Fixed id per failure: a retried copy (sweep, second replica) is a no-op, not a duplicate.
      // finishedOn tells apart two dead-letterings of the same id (a job resubmitted after replay).
      await this.dlq.add(job.name, record, {
        jobId: `${job.id}-${job.finishedOn}`,
      });
    } catch (err) {
      // The job stays in the primary queue's failed set (removeOnFail: false); the startup sweep retries it.
      this.logger.error(
        `Job ${job.id} could not be dead-lettered and remains in "${job.queueName}" failed set: ${err instanceof Error ? err.message : String(err)}`,
      );
      return;
    }

    // Only after the DLQ copy exists: removing first would lose the job if the DLQ write failed.
    // A failed remove leaves a harmless duplicate in the failed set, so it is logged, not retried.
    await job
      .remove()
      .catch((err: unknown) =>
        this.logger.warn(
          `Job ${job.id} dead-lettered but not removed from "${job.queueName}": ${err instanceof Error ? err.message : String(err)}`,
        ),
      );
    // DLQ entries are kept until someone handles them; the size in the log is what to alert on.
    const dlqSize = await this.dlq.count().catch(() => 'unknown');
    this.logger.error(
      `Job ${job.id} dead-lettered after ${job.attemptsMade} attempt(s), DLQ size ${dlqSize}: ${error.message}`,
    );
  }
}
