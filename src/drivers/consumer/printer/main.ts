import { NestFactory } from '@nestjs/core';
import { DriverPrinterModule } from './driver-printer.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(DriverPrinterModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();
}

bootstrap();
