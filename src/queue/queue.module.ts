import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Env } from '../config/env.validation';
import { DEFAULT_DLQ, DEFAULT_QUEUE } from './queue.constants';
import { QueueProducer } from './queue.producer';
import { QueueWorker } from './queue.worker';

@Module({
  imports: [
    // Shared connection settings for every queue and worker in the app.
    BullModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService<Env, true>) => ({
        connection: {
          url: config.get('REDIS_URL', { infer: true }),
          // Required by BullMQ workers: their blocking commands must wait out a Redis outage, not error.
          maxRetriesPerRequest: null,
        },
      }),
    }),
    BullModule.registerQueueAsync({
      name: DEFAULT_QUEUE,
      inject: [ConfigService],
      useFactory: (config: ConfigService<Env, true>) => ({
        defaultJobOptions: {
          attempts: config.get('JOB_ATTEMPTS', { infer: true }),
          // Exponential backoff: retry n waits up to JOB_BACKOFF_MS * 2^(n-1), i.e. 1s, 2s, 4s...
          // A struggling downstream gets breathing room instead of a retry storm.
          // Jitter 0.5 spreads each wait over 50-100% of that: jobs that failed together (an
          // outage) would otherwise all retry in the same instant.
          backoff: {
            type: 'exponential',
            delay: config.get('JOB_BACKOFF_MS', { infer: true }),
            jitter: 0.5,
          },
          // A fixed count, not an age: Redis is noeviction, so memory must not grow with traffic.
          removeOnComplete: { count: 1000 },
          // Keep failed jobs: QueueWorker removes them only after the DLQ copy is safely written.
          removeOnFail: false,
        },
      }),
    }),
    // No worker consumes the DLQ: entries wait there for inspection and manual replay.
    BullModule.registerQueue({
      name: DEFAULT_DLQ,
      defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
    }),
  ],
  providers: [QueueProducer, QueueWorker],
  exports: [QueueProducer],
})
export class QueueModule {}
