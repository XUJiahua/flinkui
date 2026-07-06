# syntax=docker/dockerfile:1

# ---- Stage 1: build the frontend static export ----
FROM node:22-alpine AS frontend
RUN corepack enable
WORKDIR /app/frontend
COPY frontend/package.json frontend/pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile || pnpm install
COPY frontend/ ./
RUN pnpm build

# ---- Stage 2: build the Go binary with the embedded frontend ----
FROM golang:1.24-alpine AS backend
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Embed the freshly-built frontend export.
RUN rm -rf web/dist && mkdir -p web/dist
COPY --from=frontend /app/frontend/out/ web/dist/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/flinkui ./cmd/server

# ---- Stage 3: minimal runtime image ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=backend /out/flinkui /flinkui
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/flinkui"]
