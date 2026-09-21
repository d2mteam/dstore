// Command coordinator là control plane: registry node, metadata file, repair.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/shardclient"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("svc", "coordinator")

	var (
		listen        = env("LISTEN_ADDR", ":9000")
		healthEvery   = envDuration("HEALTH_INTERVAL", 5*time.Second)
		healthTimeout = envDuration("HEARTBEAT_TIMEOUT", 20*time.Second)
		repairEvery   = envDuration("REPAIR_INTERVAL", 10*time.Second)
		gcEvery       = envDuration("GC_INTERVAL", 60*time.Second)
		gcGrace       = envDuration("GC_GRACE", 2*time.Minute)
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Metadata đi qua MetaStore; bản này giữ trong RAM, restart là mất.
	meta := coordinator.NewMemoryMeta()

	metrics := &coordinator.Metrics{}
	store := coordinator.NewStore(meta)
	nodes, files, err := store.Load(ctx)
	if err != nil {
		log.Error("không đọc được metadata", "err", err)
		os.Exit(1)
	}
	log.Info("đã khôi phục metadata", "nodes", nodes, "files", files)
	placer := coordinator.NewPlacer(store)
	shards := shardclient.New(30 * time.Second)

	health := &coordinator.HealthMonitor{
		Store: store, Shards: shards,
		Interval: healthEvery, Timeout: healthTimeout, Metrics: metrics, Log: log,
	}
	repairer := &coordinator.Repairer{
		Store: store, Placer: placer, Shards: shards,
		Interval: repairEvery, Metrics: metrics, Log: log,
	}
	collector := &coordinator.Collector{
		Store: store, Shards: shards,
		Interval: gcEvery, Grace: gcGrace, Metrics: metrics, Log: log,
	}
	api := &coordinator.Server{
		Store: store, Placer: placer, Health: health, Shards: shards, Metrics: metrics, Log: log,
	}

	go health.Run(ctx)
	go repairer.Run(ctx)
	go collector.Run(ctx)

	srv := &http.Server{Addr: listen, Handler: api.Routes()}
	go func() {
		log.Info("coordinator nghe HTTP", "listen", listen,
			"heartbeat_timeout", healthTimeout, "repair_interval", repairEvery,
			"gc_interval", gcEvery, "gc_grace", gcGrace)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server dừng", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("đang tắt coordinator")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
