import type http from "http";
import type { Server as HttpServer } from "http";
import WebSocket, { WebSocketServer } from "ws";
import {Redis} from "ioredis";

declare module "ws" {
  interface WebSocket {
    isAlive?: boolean;
  }
}

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const PUBSUB_CHANNEL = process.env.PUBSUB_CHANNEL ?? "discover:solana:updated";
const HEARTBEAT_INTERVAL = Number(process.env.WS_HEARTBEAT_MS ?? 30_000);

/**
 * Attach WebSocket server to an existing http.Server and return handles for shutdown.
 * This mirrors the signature used in your bootstrapApp (startSocketIoServer(server))
 */
export async function startWebServer(server: HttpServer) {
  // create a ws server that will use the existing server via 'upgrade' event
  const wss = new WebSocketServer({ noServer: true });

  // Create redis subscriber (single instance)
  const sub = new Redis(REDIS_URL);

  // helper to broadcast to open clients
  function broadcastToClients(payload: string) {
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        try {
          client.send(payload);
        } catch (err) {
          // swallow per-client errors
          console.warn("ws: failed to send to client", err);
        }
      }
    }
  }

  // wire redis subscription
  try {
    // using async/await subscribe API to avoid callback races
    await sub.subscribe(PUBSUB_CHANNEL);
    console.log(`Redis subscriber connected and subscribed to ${PUBSUB_CHANNEL}`);
  } catch (err) {
    console.error("ws: redis subscribe failed", err);
    // rethrow so caller can decide (bootstrapApp catches and continues)
    throw err;
  }

  sub.on("message", (_channel: string, message: string) => {
    // optionally validate or transform message here
    broadcastToClients(message);
  });

  // handle http upgrade requests and delegate to wss
  server.on("upgrade", (request, socket, head) => {
    // If you want path-based routing, check request.url here (e.g. only /ws)
    // Example: if (request.url !== "/ws") { socket.destroy(); return; }
    wss.handleUpgrade(request, socket as any, head, (ws) => {
      wss.emit("connection", ws, request);
    });
  });

  // per-client lifecycle
  wss.on("connection", (ws: WebSocket, req) => {
    try {
      // mark alive for heartbeat detection
      ws.isAlive = true;

      // small auth example: accept an 'auth' message from client
      const authTimeout = setTimeout(() => {
        // if you want to enforce auth, close here, else keep open
        // ws.close(4001, "auth required");
      }, 2000);

      ws.on("message", (raw) => {
        // message comes as Buffer or string
        const text = typeof raw === "string" ? raw : raw.toString();
        try {
          const msg = JSON.parse(text);
          if (msg?.type === "auth") {
            clearTimeout(authTimeout);
            // verify token here (JWT verify) if you want
            ws.send(JSON.stringify({ type: "auth_ok" }));
            return;
          }
          if (msg?.type === "pong") {
            ws.isAlive = true;
            return;
          }
          // other client messages can be handled here
        } catch (e) {
          // ignore non-json messages or handle binary payloads
        }
      });

      ws.on("pong", () => {
        ws.isAlive = true;
      });

      ws.on("close", () => {
        // client cleanup if needed
      });

      // immediately optionally send current top state or welcome payload
      // e.g., you could read from Redis zset and send initial payload here.
    } catch (err) {
      console.warn("ws: connection handler error", err);
    }
  });

  // heartbeat interval
  const hb = setInterval(() => {
    for (const client of wss.clients) {
      // types: augment WebSocket type so isAlive exists
      if (!(client as WebSocket).isAlive) {
        client.terminate();
        continue;
      }
      (client as WebSocket).isAlive = false;
      try {
        client.ping(); // triggers 'pong' if client responsive
      } catch (err) {
        // ignore
      }
    }
  }, HEARTBEAT_INTERVAL);

  // Provide a simple helper that returns a localAddress string for logging
  function localAddress() {
    // Use server.address() to get host/port if available
    try {
      const addr = server.address();
      if (!addr) return `ws://unknown`;
      if (typeof addr === "string") return addr;
      return `ws://${(addr.address || "127.0.0.1")}:${addr.port}`;
    } catch {
      return "ws://unknown";
    }
  }

  // graceful shutdown handler
  async function shutdown() {
    // stop heartbeat
    clearInterval(hb);

    // close wss (stop accepting new ws)
    await new Promise<void>((resolve) => {
      try {
        wss.close(() => resolve());
      } catch {
        resolve();
      }
    });

    // unsubscribe and quit redis
    try {
      await sub.unsubscribe(PUBSUB_CHANNEL);
    } catch (e) {
      // ignore
    }
    try {
      await sub.quit();
    } catch (e) {
      // ignore
    }

    // remove the upgrade listener we added
    // Note: Node's server.listeners('upgrade') returns array; we cannot remove exact listener unless we kept ref.
    // In many cases process is exiting; if you need to remove, consider storing the handler in a variable and calling server.off.
  }

  return {
    localAddress,
    shutdown,
  };
}
