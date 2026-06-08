import { betterAuth } from "better-auth";
import { bearer } from "better-auth/plugins";
import { Pool } from "pg";

/**
 * BRAINZ BetterAuth instance.
 *
 * Required env vars:
 *   DATABASE_URL       — Supabase PostgreSQL direct connection string
 *   BETTER_AUTH_SECRET — >=32 char random secret (shared with all brainz-agent instances)
 *   BETTER_AUTH_URL    — Public URL of this service (e.g. https://auth.storeez.studio)
 *
 * Optional env vars:
 *   GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET  — Enable Google OAuth
 *   TRUSTED_ORIGINS    — Comma-separated allowed origins (e.g. https://companion.storeez.studio)
 */
export const auth = betterAuth({
  database: new Pool({
    connectionString: process.env.DATABASE_URL!,
    max: 10,
    idleTimeoutMillis: 30000,
  }),
  secret: process.env.BETTER_AUTH_SECRET!,
  baseURL: process.env.BETTER_AUTH_URL || "http://localhost:3001",

  // Bearer plugin: enables Authorization: Bearer <token> for API clients.
  // Skills, swarm agents, and automation tools use this instead of cookies.
  plugins: [bearer()],

  // Email + password auth — always enabled.
  // The BRAINZ owner account is created via the migrate+seed step.
  emailAndPassword: {
    enabled: true,
    requireEmailVerification: false,
  },

  // Google OAuth — auto-enabled when GOOGLE_CLIENT_ID is set.
  ...(process.env.GOOGLE_CLIENT_ID
    ? {
        socialProviders: {
          google: {
            clientId: process.env.GOOGLE_CLIENT_ID!,
            clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
          },
        },
      }
    : {}),

  // CORS: list origins that may call the auth service directly.
  trustedOrigins: (process.env.TRUSTED_ORIGINS || "")
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean),

  // Session lifetime: 30-day tokens, auto-refresh on use.
  session: {
    expiresIn: 60 * 60 * 24 * 30,
    updateAge: 60 * 60 * 24,
  },
});
