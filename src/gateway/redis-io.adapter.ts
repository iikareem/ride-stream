import { INestApplication, Logger } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createShardedAdapter } from '@socket.io/redis-adapter';
import { ServerOptions } from 'socket.io';
import Redis from 'ioredis';
import { redisConfig } from '../redis/redis.config';

/**
 * Sharded Redis adapter (Redis 7+ SPUBLISH/SSUBSCRIBE).
 * With subscriptionMode "dynamic", a gateway only receives emits for rooms
 * it actually has — so 2–3 gateway processes don't all see every push.
 */
export class RedisIoAdapter extends IoAdapter {
  private readonly logger = new Logger(RedisIoAdapter.name);
  private adapterConstructor: ReturnType<typeof createShardedAdapter> | null =
    null;

  constructor(app: INestApplication) {
    super(app);
  }

  async connectToRedis(): Promise<void> {
    // Subscriber clients must use maxRetriesPerRequest: null (ioredis + Socket.IO)
    const pubClient = new Redis(redisConfig.url, {
      maxRetriesPerRequest: null,
    });
    const subClient = pubClient.duplicate();

    await Promise.all([whenReady(pubClient), whenReady(subClient)]);

    // Learning: log when Redis delivers a sharded pub/sub message to THIS gateway.
    // (Adapter still handles the real decode → socket delivery.)
    subClient.on('smessageBuffer', (channel: Buffer, message: Buffer) => {
      this.logger.log(
        `redis SMESSAGE channel=${channel.toString()} bytes=${message.length}`,
      );
    });

    this.adapterConstructor = createShardedAdapter(pubClient, subClient, {
      // One channel per public room → only nodes that joined user:{id} get that emit
      subscriptionMode: 'dynamic',
    });
    this.logger.log(
      `Socket.IO sharded Redis adapter ready (${redisConfig.url}, mode=dynamic)`,
    );
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
