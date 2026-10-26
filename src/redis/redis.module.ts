import {
  Global,
  Inject,
  Logger,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Redis } from 'ioredis';
import type { Env } from '../config/env.validation';

export const REDIS_CLIENT = Symbol('REDIS_CLIENT');

const logger = new Logger('Redis');

/**
 * Client for the rate limiter only. BullMQ opens its own connections because it needs the
 * opposite settings: workers block on Redis and must retry forever, the rate limiter must fail fast.
 */
@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [ConfigService],
      useFactory: (config: ConfigService<Env, true>): Redis => {
        const redis = new Redis(config.get('REDIS_URL', { infer: true }), {
          // Fail fast in an outage; the default (20) queues each command for ~70s of reconnect retries.
          maxRetriesPerRequest: 1,
          // A frozen Redis keeps the socket open but silent: without this, requests hang.
          commandTimeout: 500,
          connectTimeout: 1000,
        });
        redis.on('error', (err: Error) => logger.error(err.message));
        return redis;
      },
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async onApplicationShutdown(): Promise<void> {
    await this.redis.quit();
  }
}
