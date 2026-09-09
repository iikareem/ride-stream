import { NestFactory } from '@nestjs/core';
import { EtaModule } from './eta.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(EtaModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
