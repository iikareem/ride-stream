import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { GatewayModule } from './gateway.module';
import { gatewayConfig } from './gateway.config';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(GatewayModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();

  const port = gatewayConfig.port;
  await app.listen(port);

  const logger = new Logger('Gateway');
  logger.log(
    `WebSocket gateway listening on http://localhost:${port} (Socket.IO + Redis Pub/Sub; emit join { userId })`,
  );
}

bootstrap();
