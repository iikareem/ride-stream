import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { RedisService } from '../shared/redis/redis.service';
import { userChannel } from '../shared/redis/redis.config';

type JoinPayload = {
  userId?: string;
};

/**
 * Socket.IO gateway: join user:{id} room and SUBSCRIBE the matching Redis channel.
 * On PUBLISH, forward the payload to that room as event `drivers`.
 */
@WebSocketGateway({
  cors: { origin: '*' },
})
export class EventsGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(EventsGateway.name);

  @WebSocketServer()
  server!: Server;

  constructor(private readonly redis: RedisService) {}

  afterInit(): void {
    this.redis.onUserChannelMessage((channel, message) => {
      this.logger.log(
        `redis PUBLISH received channel=${channel} message=${message}`,
      );
      const payload = tryParse(message);
      this.server.to(channel).emit('drivers', payload);
    });
  }

  handleConnection(client: Socket): void {
    this.logger.log(`connected socketId=${client.id}`);

    client.onAnyOutgoing((event, ...args) => {
      this.logger.log(
        `deliver socketId=${client.id} userId=${client.data.userId ?? 'none'} event=${event} payload=${safeJson(args[0])}`,
      );
    });
  }

  async handleDisconnect(client: Socket): Promise<void> {
    const userId = client.data.userId as string | undefined;
    this.logger.log(
      `disconnected socketId=${client.id} userId=${userId ?? 'none'}`,
    );
    if (userId) {
      await this.redis.unsubscribeUser(userId);
    }
  }

  @SubscribeMessage('join')
  async handleJoin(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: JoinPayload,
  ): Promise<{ ok: boolean; room?: string; channel?: string; error?: string }> {
    const userId = body?.userId?.trim();
    if (!userId) {
      return { ok: false, error: 'userId is required' };
    }

    const room = userChannel(userId);
    void client.join(room);
    client.data.userId = userId;

    const channel = await this.redis.subscribeUser(userId);

    this.logger.log(
      `join socketId=${client.id} userId=${userId} room=${room} redisChannel=${channel}`,
    );

    return { ok: true, room, channel };
  }
}

function tryParse(message: string): unknown {
  try {
    return JSON.parse(message) as unknown;
  } catch {
    return { text: message };
  }
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
