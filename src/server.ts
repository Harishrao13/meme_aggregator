import http from "http";
import type { Server as HttpServer } from "http";
import app from "./app.js";
import { startWebServer } from "./services/websocket.service.js";
import { startRanker } from "./workers/ranking.worker.js";
import { startDiscovery } from "./workers/discovery.worker.js";
import { startDeltaWorker } from "./workers/delta.worker.js";

type ShutdownHandler = () => Promise<void>;

/**
 * Create and start the HTTP server (does not attach sockets/workers).
 * Returns the http.Server instance so callers (tests) can inspect/listen/close.
 */
export function startServer(port = Number(process.env.PORT ?? 8080)): HttpServer {
  const server = http.createServer(app);
  server.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
  });
  return server;
}

/**
 * Full application bootstrap: start server, attach socket/io, and start background workers.
 * This performs best-effort startup and returns handles for shutdown.
 */
export async function bootstrapApp(): Promise<{
  server: HttpServer;
  socketHandles?: Awaited<ReturnType<typeof startWebServer>>;
  stop: ShutdownHandler;
}> {
  const server = startServer();

  // Container for resources that need shutdown
  const resources: Array<ShutdownHandler> = [];

  let socketHandles;
  try {
    socketHandles = await startWebServer(server);
    resources.push(socketHandles.shutdown);
    console.log("Socket.IO attached at:", socketHandles.localAddress());
  } catch (err) {
    console.error("Failed to attach Socket.IO server:", (err as Error).message ?? err);
  }

  // Start ranker & discovery workers (fire-and-forget but capture shutdown if they expose it)
  // If your workers return a shutdown function, push it into resources for graceful shutdown.
  // Here we assume startRanker/startDiscovery return Promise<void> and run indefinitely.
  startRanker().catch((err) => {
    // fatal in worker -> log and exit
    // eslint-disable-next-line no-console
    console.error("Ranker worker failed to start or crashed:", err);
    process.exit(1);
  });

  startDiscovery().catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Discovery worker failed to start or crashed:", err);
    process.exit(1);
  });

  startDeltaWorker().catch((err) =>{
    console.error("Delta Worker failed to start or crashed:",err);
    process.exit(1);
  })

  // Build a stop handler that tries to shut down resources in reverse order
  const stop: ShutdownHandler = async () => {
    // stop accepting new connections
    try {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
      // eslint-disable-next-line no-console
      console.log("HTTP server closed");
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn("Error closing HTTP server:", (err as Error).message ?? err);
    }

    // run other resource shutdowns (socket, redis) sequentially
    for (const shutdown of resources.reverse()) {
      try {
        await shutdown();
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn("Error during shutdown:", (err as Error).message ?? err);
      }
    }
  };

  return { server, socketHandles, stop };
}

/**
 * If RUN_SERVER !== "false" bootstrap on process start.
 */
if (process.env.RUN_SERVER !== "false") {
  (async () => {
    const { server, socketHandles, stop } = await bootstrapApp();

    // Graceful shutdown wiring
    const graceful = async (signal?: string) => {
      // eslint-disable-next-line no-console
      console.log(`Received ${signal ?? "shutdown"} - closing gracefully...`);
      try {
        await stop();
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error("Error during graceful shutdown:", (err as Error).message ?? err);
      } finally {
        // give logs a moment then exit; host orchestrator is responsible for restarts
        setTimeout(() => process.exit(0), 50);
      }
    };

    process.once("SIGINT", () => graceful("SIGINT"));
    process.once("SIGTERM", () => graceful("SIGTERM"));

    // Also handle uncaught errors -> attempt graceful shutdown then exit nonzero
    process.once("uncaughtException", async (err) => {
      // eslint-disable-next-line no-console
      console.error("Uncaught exception:", err);
      try {
        await stop();
      } finally {
        process.exit(1);
      }
    });

    process.once("unhandledRejection", async (reason) => {
      // eslint-disable-next-line no-console
      console.error("Unhandled rejection:", reason);
      try {
        await stop();
      } finally {
        process.exit(1);
      }
    });
  })().catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to bootstrap app:", (err as Error).message ?? err);
    process.exit(1);
  });
}

export default startServer;
