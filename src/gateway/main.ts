import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'path';
import { GatewayModule } from './gateway.module';
import { gatewayConfig } from './gateway.config';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(GatewayModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();

  const clientDir = join(process.cwd(), 'client');
  app.useStaticAssets(clientDir);

  const port = gatewayConfig.port;
  await app.listen(port);

  const logger = new Logger('Gateway');
  logger.log(
    `WebSocket gateway listening on http://localhost:${port} (UI + Socket.IO; join { userId })`,
  );
  logger.log(`Live feed UI: http://localhost:${port}/`);
}

bootstrap();
