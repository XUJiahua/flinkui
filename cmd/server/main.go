// Command server is the single-binary Flink job management platform: it serves
// the REST/WebSocket API and the embedded frontend (design §3.5 "部署").
package main

import (
	"context"
	"flag"
	"io/fs"
	"log"

	"github.com/fko-demo/flinkui/internal/api"
	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/fko-demo/flinkui/web"
)

func main() {
	configFile := flag.String("config", "", "path to config file (optional; env FKO_* also supported)")
	flag.Parse()

	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Cluster accessor (in-cluster if kubeconfig empty; else kubeconfig).
	acc, err := cluster.NewKubeAccessor(
		cfg.Cluster.Name,
		cfg.Cluster.Namespace,
		cfg.Cluster.Kubeconfig,
		cfg.Cluster.Context,
	)
	if err != nil {
		log.Fatalf("init cluster accessor: %v", err)
	}

	svc := flink.NewService(acc, cfg)

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

	// Embedded frontend rooted at web/dist.
	staticFS, err := fs.Sub(web.Dist, "dist")
	if err != nil {
		log.Fatalf("mount embedded frontend: %v", err)
	}

	srv := api.New(cfg, svc, st, a, staticFS)
	log.Printf("Flink job console listening on %s (cluster=%s namespace=%s)",
		cfg.Addr, cfg.Cluster.Name, cfg.Cluster.Namespace)
	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
