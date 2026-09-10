import { NestFactory } from '@nestjs/core';
import { NearbyModule } from './nearby.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(NearbyModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
