import "dotenv/config";
import express from "express";
import { Server } from "http";
import { ApolloServer } from "@apollo/server";
import { expressMiddleware } from "@apollo/server/express4";
import cors from "cors";
import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import { PrismaClient } from "./generated/prisma/client";

const app = express();
const PORT = Number(process.env.PORT) || 3000;

const adapter = new PrismaBetterSqlite3({ url: "file:./prisma/dev.db" });
const prisma = new PrismaClient({ adapter });

const typeDefs = `#graphql
  type User {
    id: Int!
    name: String!
    email: String!
  }

  type Query {
    users: [User!]!
    user(id: Int!): User
  }

  type Mutation {
    createUser(name: String!, email: String!): User!
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
      console.log("Stopping Apollo Server...");
      await apollo.stop();
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
