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

const apollo = new ApolloServer({ typeDefs, resolvers });

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

  let shuttingDown = false;

  async function gracefulShutdown(signal: string) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\nReceived ${signal}. Waiting 5 seconds before shutting down...`);

    setTimeout(async () => {
      try {
        console.log("Stopping Apollo Server...");
        await apollo.stop();

        console.log("Closing BullMQ worker...");
        await notificationWorker.close();

        console.log("Closing BullMQ queue...");
        await notificationQueue.close();

        console.log("Disconnecting Redis...");
        redis.disconnect();

        console.log("Disconnecting Prisma...");
        await prisma.$disconnect();

        console.log("Closing HTTP server...");
        httpServer.close((err) => {
          if (err) {
            console.error("Error during shutdown:", err);
            process.exit(1);
          }
          console.log("Server closed gracefully");
          process.exit(0);
        });
      } catch (err) {
        console.error("Error during shutdown:", err);
        process.exit(1);
      }

      setTimeout(() => {
        console.error("Forced shutdown after timeout");
        process.exit(1);
      }, 5000);
    }, 5000);
  }

  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
  process.on("SIGINT", () => gracefulShutdown("SIGINT"));
}

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
