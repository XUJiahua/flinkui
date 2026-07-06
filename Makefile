# Flink Job Console — build orchestration.
# The frontend is statically exported and embedded into the Go binary so the
# whole platform ships as a single executable / image (design §3.5 "部署").

BINARY      := bin/flinkui
FRONTEND    := frontend
WEB_DIST    := web/dist
IMAGE       ?= flinkui:latest

.PHONY: all frontend backend build run test vet clean docker tidy

all: build

## frontend: build the Next.js static export and copy it into web/dist for embedding.
frontend:
	cd $(FRONTEND) && pnpm install --frozen-lockfile || (cd $(FRONTEND) && pnpm install)
	cd $(FRONTEND) && pnpm build
	rm -rf $(WEB_DIST)
	mkdir -p $(WEB_DIST)
	cp -r $(FRONTEND)/out/* $(WEB_DIST)/

## backend: compile the Go binary (embeds whatever is currently in web/dist).
backend:
	go build -o $(BINARY) ./cmd/server

## build: full pipeline — frontend first (CI ordering), then backend embed.
build: frontend backend

## run: run the server locally (expects FKO_* env or -config file).
run:
	go run ./cmd/server

## test: run Go tests.
test:
	go test ./...

## vet: static analysis.
vet:
	go vet ./...

## tidy: resolve go modules.
tidy:
	go mod tidy

## docker: build the single-image deliverable.
docker:
	docker build -t $(IMAGE) .

## clean: remove build artifacts.
clean:
	rm -rf $(BINARY) $(FRONTEND)/out $(FRONTEND)/.next
