import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { GatewayModule } from './gateway.module';
import { gatewayConfig } from './gateway.config';
import { RedisIoAdapter } from './redis-io.adapter';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(GatewayModule, {
    logger: ['log', 'error', 'warn'],
  });
  app.enableShutdownHooks();

  const redisIoAdapter = new RedisIoAdapter(app);
  await redisIoAdapter.connectToRedis();
  app.useWebSocketAdapter(redisIoAdapter);

  const port = gatewayConfig.port;
  await app.listen(port);

  const logger = new Logger('Gateway');
  logger.log(
    `WebSocket gateway listening on http://localhost:${port} (Socket.IO + Redis adapter only; emit join { userId })`,
  );
}

bootstrap();
