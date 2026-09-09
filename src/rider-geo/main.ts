import { NestFactory } from '@nestjs/core';
import { RiderGeoModule } from './rider-geo.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(RiderGeoModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
