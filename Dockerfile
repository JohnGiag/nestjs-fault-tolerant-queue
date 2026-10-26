# Build stage: full dependencies to compile TypeScript.
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build && npm prune --omit=dev

# Runtime stage: compiled JS and production dependencies only, as a non-root user.
FROM node:24-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /app/package.json ./
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
USER node
EXPOSE 3000
# Any HTTP response (even 404) means the server is up; no Redis call, so an outage doesn't flap health.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s \
  CMD node -e "fetch('http://127.0.0.1:3000/').then(() => process.exit(0), () => process.exit(1))"
# Exec form: node is PID 1 and gets SIGTERM directly, so workers drain active jobs on docker stop.
CMD ["node", "dist/main.js"]
