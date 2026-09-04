import { NestFactory } from '@nestjs/core';
import { LiveMapModule } from './live-map.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(LiveMapModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
