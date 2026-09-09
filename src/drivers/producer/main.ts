import { NestFactory } from '@nestjs/core';
import { DriverProducerModule } from './driver-producer.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(DriverProducerModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
