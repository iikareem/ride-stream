import { NestFactory } from '@nestjs/core';
import { RiderProducerModule } from './rider-producer.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(RiderProducerModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
