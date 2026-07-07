// Command server is the single-binary Flink job management platform: it serves
// the REST/WebSocket API and the embedded frontend (design §3.5 "部署").
package main

import (
	"context"
	"flag"
	"io/fs"
	"log"
	"time"

	"github.com/fko-demo/flinkui/internal/api"
	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/failover"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/fko-demo/flinkui/web"
)

// version is set at build time via -ldflags "-X main.version=<tag>".
var version = "dev"

func main() {
	configFile := flag.String("config", "", "path to config file (optional; env FKO_* also supported)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		log.Printf("flinkui %s", version)
		return
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Cluster accessor (in-cluster if kubeconfig empty; else kubeconfig).
	var acc cluster.ClusterAccessor
	kubeAcc, err := cluster.NewKubeAccessor(
		cfg.Cluster.Name,
		cfg.Cluster.Namespace,
		cfg.Cluster.Kubeconfig,
		cfg.Cluster.Context,
	)
	if err != nil {
		log.Fatalf("init cluster accessor: %v", err)
	}
	acc = kubeAcc

	svc := flink.NewService(acc, cfg)

	// Start informer-backed caching if the accessor supports it (design §3.3).
	// Best-effort: on failure we log and fall back to live API listing.
	if starter, ok := acc.(cluster.Starter); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := starter.Start(ctx); err != nil {
			log.Printf("warning: informer cache sync failed, using live API listing: %v", err)
		} else {
			log.Printf("informer cache synced for FlinkDeployments")
		}
		cancel()
	}

	// S3 store is optional; log and continue if unconfigured.
	var st *store.Store
	if cfg.Cluster.S3.Endpoint != "" || cfg.Cluster.S3.AccessKey != "" {
		st, err = store.New(context.Background(), cfg.Cluster.S3)
		if err != nil {
			log.Printf("warning: S3 store disabled: %v", err)
			st = nil
		}
	} else {
		log.Printf("warning: S3 not configured; rollback recovery-point listing disabled")
	}

	a := auth.New(cfg.Auth)
	if cfg.Auth.Password == "" {
		log.Printf("WARNING: auth password is empty; set FKO_AUTH_PASSWORD to secure the platform")
	}

	// Failover / HA groups (design failover P1). Enabled when groups are declared.
	var fo *failover.Service
	if len(cfg.HAGroups) > 0 {
		reg := cluster.NewRegistry(cfg)
		fo = failover.NewService(cfg, reg)
		log.Printf("failover enabled: %d HA group(s) declared", len(cfg.HAGroups))
	}

	// Embedded frontend rooted at web/dist.
	staticFS, err := fs.Sub(web.Dist, "dist")
	if err != nil {
		log.Fatalf("mount embedded frontend: %v", err)
	}

	srv := api.New(cfg, svc, st, fo, a, staticFS)
	log.Printf("Flink job console %s listening on %s (cluster=%s namespace=%s)",
		version, cfg.Addr, cfg.Cluster.Name, cfg.Cluster.Namespace)
	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
