import { Module } from '@nestjs/common';
import { QueueModule } from '../queue/queue.module';
import { RateLimiterModule } from '../rate-limiter/rate-limiter.module';
import { TasksController } from './tasks.controller';

@Module({
  imports: [QueueModule, RateLimiterModule],
  controllers: [TasksController],
})
export class TasksModule {}
