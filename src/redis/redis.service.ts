import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { redisConfig } from './redis.config';

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client: Redis | null = null;

  async onModuleInit(): Promise<void> {
    this.client = new Redis(redisConfig.url, {
      maxRetriesPerRequest: 3,
    });
    await this.client.ping();
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

  async onModuleDestroy(): Promise<void> {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }
}
