import { toNodeHandler } from "better-auth/node";
import { auth } from "./auth";
import http from "node:http";

const PORT = parseInt(process.env.PORT || "3001", 10);

const server = http.createServer(toNodeHandler(auth));

server.listen(PORT, () => {
  const url = process.env.BETTER_AUTH_URL || `http://localhost:${PORT}`;
  console.log(`[BetterAuth] Listening on :${PORT}`);
  console.log(`[BetterAuth] Base URL      : ${url}`);
  console.log(`[BetterAuth] Sign-in       : ${url}/api/auth/sign-in/email`);
  console.log(`[BetterAuth] Get session   : ${url}/api/auth/get-session`);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  server.close(() => process.exit(0));
});
