declare module "@apollo/server/express4" {
  import type { RequestHandler } from "express";
  import type { ApolloServer, BaseContext } from "@apollo/server";

  export function expressMiddleware<
    TContext extends BaseContext = BaseContext,
  >(
    server: ApolloServer<TContext>,
    options?: {
      context?: (ctx: { req: import("express").Request }) => Promise<TContext>;
    },
  ): RequestHandler;
}
