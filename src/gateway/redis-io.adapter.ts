import { INestApplication, Logger } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import { ServerOptions } from 'socket.io';
import Redis from 'ioredis';
import { redisConfig } from '../redis/redis.config';

/**
 * Classic Redis adapter — pairs with @socket.io/redis-emitter
 * (PUBLISH + Socket.IO packet format). Use this for multi-gateway + emitter.
 */
export class RedisIoAdapter extends IoAdapter {
  private readonly logger = new Logger(RedisIoAdapter.name);
  private adapterConstructor: ReturnType<typeof createAdapter> | null = null;

  constructor(app: INestApplication) {
    super(app);
  }

  async connectToRedis(): Promise<void> {
    const pubClient = new Redis(redisConfig.url, {
      maxRetriesPerRequest: null,
    });
    const subClient = pubClient.duplicate();

    await Promise.all([whenReady(pubClient), whenReady(subClient)]);

    subClient.on('message', (channel: string, message: string) => {
      this.logger.log(
        `redis adapter message channel=${channel} bytes=${Buffer.byteLength(message)}`,
      );
    });

    this.adapterConstructor = createAdapter(pubClient, subClient);
    this.logger.log(`Socket.IO Redis adapter ready (${redisConfig.url})`);
  }

  createIOServer(port: number, options?: ServerOptions) {
    const server = super.createIOServer(port, options);
    if (!this.adapterConstructor) {
      throw new Error(
        'Redis adapter not initialized — call connectToRedis() first',
      );
    }
    server.adapter(this.adapterConstructor);
    return server;
  }
}

function whenReady(client: Redis): Promise<void> {
  if (client.status === 'ready') {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    client.once('ready', () => resolve());
    client.once('error', reject);
  });
}
