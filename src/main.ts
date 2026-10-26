import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';
import type { Env } from './config/env.validation';
import { applyTrustProxy } from './trust-proxy';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  const config = app.get(ConfigService<Env, true>);
  // Behind a load balancer, req.ip is the balancer's address unless trusted: every client would share one rate limit.
  applyTrustProxy(app, config.get('TRUST_PROXY_HOPS', { infer: true }));
  // On SIGTERM, lets workers finish their active jobs and closes Redis instead of dropping them mid-run.
  app.enableShutdownHooks();
  await app.listen(config.get('PORT', { infer: true }));
}
void bootstrap();
