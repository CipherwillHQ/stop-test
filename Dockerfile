FROM node:22-bookworm-slim AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/
COPY prisma/ ./prisma/
COPY prisma.config.ts ./

ENV DATABASE_URL="file:./dev.db"
RUN npx prisma generate
RUN npx prisma db push
RUN npm run build
RUN npm prune --omit=dev

FROM node:22-bookworm-slim AS runner

WORKDIR /app

COPY --from=builder /app/node_modules/ ./node_modules/
COPY --from=builder /app/dist/ ./dist/
COPY --from=builder /app/dev.db ./dev.db
COPY --from=builder /app/package.json ./package.json

EXPOSE 3000

ENV DATABASE_URL="file:./dev.db"

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["node", "dist/healthcheck.js"]

CMD ["yarn", "start"]
