// Package web embeds the statically-exported frontend (design "部署/单二进制").
// The frontend build (pnpm build -> out/) is copied into dist/ before go build.
package web

import "embed"

// Dist holds the embedded frontend static export. The all: prefix ensures files
// starting with "_" (e.g. Next.js _next/) are included.
//
//go:embed all:dist
var Dist embed.FS
