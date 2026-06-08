FROM oven/bun:1-alpine
WORKDIR /app

# Install dependencies first (cached layer)
COPY package.json ./
RUN bun install --frozen-lockfile 2>/dev/null || bun install

# Copy source
COPY tsconfig.json ./
COPY src ./src

EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://localhost:3001/api/auth/ok || exit 1

CMD ["bun", "src/index.ts"]
