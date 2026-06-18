import "dotenv/config";
import express from "express";
import { Server } from "http";
import { ApolloServer } from "@apollo/server";
import { expressMiddleware } from "@apollo/server/express4";
import cors from "cors";
import Redis from "ioredis";
import { Queue, Worker, Job } from "bullmq";
import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import { PrismaClient } from "./generated/prisma/client";

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });

const adapter = new PrismaBetterSqlite3({ url: process.env["DATABASE_URL"] || "file:./dev.db" });
const prisma = new PrismaClient({ adapter });

const redisUrl = new URL(REDIS_URL);
const connection = {
  host: redisUrl.hostname,
  port: Number(redisUrl.port) || 6379,
  password: redisUrl.password || undefined,
};

const notificationQueue = new Queue("notifications", { connection });
const notificationWorker = new Worker(
  "notifications",
  async (job: Job) => {
    console.log(`Processing notification job ${job.id}:`, job.data);
    await new Promise((resolve) => setTimeout(resolve, 1000));
    console.log(`Notification job ${job.id} completed`);
  },
  { connection },
);

notificationWorker.on("completed", (job: Job) => {
  console.log(`Job ${job.id} completed successfully`);
});

notificationWorker.on("failed", (job: Job | undefined, err: Error) => {
  console.error(`Job ${job?.id} failed:`, err.message);
});

const typeDefs = `#graphql
  type User {
    id: Int!
    name: String!
    email: String!
  }

  type JobResult {
    jobId: String!
    message: String!
  }

  type Query {
    users: [User!]!
    user(id: Int!): User
  }

  type Mutation {
    createUser(name: String!, email: String!): User!
    enqueueNotification(message: String!): JobResult!
  }
`;

const resolvers = {
  Query: {
    users: () => prisma.user.findMany(),
    user: (_: unknown, args: { id: number }) =>
      prisma.user.findUnique({ where: { id: args.id } }),
  },
  Mutation: {
    createUser: (_: unknown, args: { name: string; email: string }) =>
      prisma.user.create({ data: { name: args.name, email: args.email } }),
    enqueueNotification: async (_: unknown, args: { message: string }) => {
      const job = await notificationQueue.add("send-notification", {
        message: args.message,
        timestamp: new Date().toISOString(),
      });
      return {
        jobId: job.id!,
        message: `Notification job ${job.id} queued`,
      };
    },
  },
};

const apollo = new ApolloServer({ typeDefs, resolvers, stopOnTerminationSignals: false });

async function start() {
  await apollo.start();

  app.use(cors());
  app.use(express.json());
  app.use("/graphql", expressMiddleware(apollo));

  app.get("/", (_req, res) => {
    res.send("Server is running");
  });

  app.get("/health", async (_req, res) => {
    try {
      await prisma.$queryRaw`SELECT 1`;
      await redis.ping();
      res.status(200).send("OK");
    } catch {
      res.status(503).send("Unhealthy");
    }
  });

  const httpServer: Server = app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
    console.log(`GraphQL endpoint: http://localhost:${PORT}/graphql`);
  });

  // Simulate backend's slow startup (DB connect, migrations, Apollo drain plugin setup, cron init)
  // During this gap, if SIGTERM arrives there is NO handler registered → exit 143.
  // Set STARTUP_DELAY_MS env var to control the gap.
  const startupDelay = Number(process.env["STARTUP_DELAY_MS"]) || 0;
  if (startupDelay > 0) {
    console.log(`[Startup] Simulating backend init gap of ${startupDelay}ms (handler not yet registered)...`);
    await new Promise((resolve) => setTimeout(resolve, startupDelay));
    console.log("[Startup] Init complete. Registering shutdown handlers now.");
  }

  let isShuttingDown = false;

  async function flushStdoutAndStderr() {
    await new Promise<void>((resolve) => {
      let completed = 0;
      const done = () => {
        if (++completed === 2) resolve();
      };
      if (!process.stdout.write("")) {
        process.stdout.once("drain", done);
      } else {
        process.nextTick(done);
      }
      if (!process.stderr.write("")) {
        process.stderr.once("drain", done);
      } else {
        process.nextTick(done);
      }
    });
  }

  function withTimeout<T>(promise: Promise<T>, timeoutMs: number, errorMessage: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => setTimeout(() => reject(new Error(errorMessage)), timeoutMs)),
    ]);
  }

  async function handleShutdown(signal: string) {
    if (isShuttingDown) {
      console.log(`[Shutdown] Received ${signal}, but shutdown is already in progress... | Process PID: ${process.pid}`);
      return;
    }

    isShuttingDown = true;
    console.log(`[Shutdown] Received ${signal}. Starting graceful shutdown... | Process PID: ${process.pid}`);

    // === DIAGNOSTIC: Test shutdown window ===
    const shutdownDelay = Number(process.env["SHUTDOWN_DELAY_MS"]) || 0;
    if (shutdownDelay > 0) {
      process.stdout.write(`[Shutdown] Sleeping ${shutdownDelay}ms to test shutdown window...\n`);
      await new Promise((resolve) => setTimeout(resolve, shutdownDelay));
      process.stdout.write("[Shutdown] Sleep done. Proceeding with shutdown.\n");
    }
    // === END DIAGNOSTIC ===

    // Disable offline queueing on Redis connection during shutdown
    // NOTE: this causes redis.quit() to fail with "Stream isn't writeable" if connection is already bad.
    // Consider removing this or catching the error gracefully.
    // if (redis.options) {
    //   redis.options.enableOfflineQueue = false;
    // }

    // Set a watchdog timeout of 28 seconds
    const watchdog = setTimeout(async () => {
      console.error("[Shutdown] Graceful shutdown watchdog timed out. Forcing process exit.");
      await flushStdoutAndStderr();
      process.exit(1);
    }, 28000);
    watchdog.unref();

    let hasErrors = false;

    const tasks = [
      {
        name: "BullMQ Worker",
        timeout: 10000,
        run: async () => {
          try {
            await notificationWorker.close();
          } finally {
            try { redis.disconnect(); } catch (_err) {}
          }
        },
      },
      {
        name: "BullMQ Queue",
        timeout: 2000,
        run: () => notificationQueue.close(),
      },
      {
        name: "Redis Connection",
        timeout: 2000,
        run: () => redis.quit(),
      },
      {
        name: "Prisma Client",
        timeout: 2000,
        run: () => prisma.$disconnect(),
      },
      {
        name: "HTTP and Apollo Server",
        timeout: 3000,
        run: async () => {
          if (httpServer) {
            httpServer.close();
            if (typeof (httpServer as any).closeAllConnections === "function") {
              (httpServer as any).closeAllConnections();
            }
          }
          await apollo.stop();
        },
      },
    ];

    for (const task of tasks) {
      try {
        console.log(`[Shutdown] Stopping ${task.name}...`);
        await withTimeout(Promise.resolve(task.run()), task.timeout, `Timeout stopping ${task.name}`);
        console.log(`[Shutdown] ${task.name} stopped.`);
      } catch (err) {
        hasErrors = true;
        console.error(
          `[Shutdown] Failed to stop ${task.name}:`,
          err instanceof Error ? err.message : String(err)
        );
      }
    }

    console.log("[Shutdown] Graceful shutdown completed successfully.");
    clearTimeout(watchdog);
    await flushStdoutAndStderr();
    process.exit(hasErrors ? 1 : 0);
  }

  process.on("unhandledRejection", (reason) => {
    process.stdout.write(`[Shutdown] Unhandled rejection during shutdown: ${reason}\n`);
  });

  process.on("SIGINT", () => {
    if (isShuttingDown) {
      process.stdout.write("[Shutdown] SECOND SIGINT received. Force exit.\n");
      process.exit(1);
    }
    return handleShutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    if (isShuttingDown) {
      process.stdout.write("[Shutdown] SECOND SIGTERM received. Force exit.\n");
      process.exit(1);
    }
    return handleShutdown("SIGTERM");
  });
}

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
