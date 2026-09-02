import { NestFactory } from '@nestjs/core';
import { ProducerModule } from './producer.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(ProducerModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
