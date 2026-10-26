import {
  BadRequestException,
  Body,
  Controller,
  Headers,
  HttpCode,
  HttpStatus,
  Post,
} from '@nestjs/common';
import { QueueProducer } from '../queue/queue.producer';
import { RateLimit } from '../rate-limiter/decorators/rate-limit.decorator';
import { parseCreateTask } from './create-task.dto';

@Controller('jobs')
export class TasksController {
  constructor(private readonly producer: QueueProducer) {}

  /**
   * The rate limit runs as a guard, before the body is even parsed into a job, so a flood is
   * rejected with 429 at the edge instead of filling Redis with jobs. Fails closed (503) if
   * Redis is down, since the queue would be unreachable anyway.
   */
  @Post()
  @RateLimit({ limit: 100, ttlSec: 60 })
  @HttpCode(HttpStatus.ACCEPTED)
  async create(
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey?: string,
  ): Promise<{ jobId: string }> {
    if (idempotencyKey !== undefined && !/^.{1,255}$/.test(idempotencyKey)) {
      throw new BadRequestException('Idempotency-Key must be 1-255 characters');
    }
    const { payload, options } = parseCreateTask(body);
    const jobId = await this.producer.enqueue(payload, {
      ...options,
      idempotencyKey,
    });
    return { jobId };
  }
}
