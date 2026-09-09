import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { redisConfig, userChannel } from './redis.config';

type MessageHandler = (channel: string, message: string) => void;

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client: Redis | null = null;
  /** Dedicated connection for SUBSCRIBE (cannot share with command client). */
  private subClient: Redis | null = null;
  private messageHandler: MessageHandler | null = null;
  private readonly channelRefCount = new Map<string, number>();

  async onModuleInit(): Promise<void> {
    this.client = new Redis(redisConfig.url, {
      maxRetriesPerRequest: 3,
    });
    await this.client.ping();

    this.subClient = new Redis(redisConfig.url, {
      maxRetriesPerRequest: null,
    });
    this.subClient.on('message', (channel: string, message: string) => {
      this.messageHandler?.(channel, message);
    });

    this.logger.log(`Connected to Redis at ${redisConfig.url}`);
  }

  getClient(): Redis {
    if (!this.client) {
      throw new Error('Redis client not connected');
    }
    return this.client;
  }

  /**
   * Upsert a member in a Redis GEO index.
   * Redis GEOADD argument order is longitude, latitude (not lat/lon).
   */
  async geoAdd(
    key: string,
    longitude: number,
    latitude: number,
    member: string,
  ): Promise<number> {
    return this.getClient().geoadd(key, longitude, latitude, member);
  }

  /** Gateway registers one handler for all user-channel messages. */
  onUserChannelMessage(handler: MessageHandler): void {
    this.messageHandler = handler;
  }

  /**
   * SUBSCRIBE to user:{userId} (refcount if several sockets share the same user).
   * This is the channel Redis Insight can PUBLISH to.
   */
  async subscribeUser(userId: string): Promise<string> {
    if (!this.subClient) {
      throw new Error('Redis subscriber not connected');
    }
    const channel = userChannel(userId);
    const prev = this.channelRefCount.get(channel) ?? 0;
    this.channelRefCount.set(channel, prev + 1);
    if (prev === 0) {
      await this.subClient.subscribe(channel);
      this.logger.log(`Redis SUBSCRIBE ${channel}`);
    }
    return channel;
  }

  async unsubscribeUser(userId: string): Promise<void> {
    if (!this.subClient) {
      return;
    }
    const channel = userChannel(userId);
    const prev = this.channelRefCount.get(channel) ?? 0;
    if (prev <= 1) {
      this.channelRefCount.delete(channel);
      await this.subClient.unsubscribe(channel);
      this.logger.log(`Redis UNSUBSCRIBE ${channel}`);
    } else {
      this.channelRefCount.set(channel, prev - 1);
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.subClient) {
      await this.subClient.quit();
      this.subClient = null;
    }
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }
}
