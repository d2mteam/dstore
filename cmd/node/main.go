// Command node là data plane: lưu shard trên đĩa và gửi heartbeat cho coordinator.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"dstore-demo/internal/node"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var (
		nodeID   = env("NODE_ID", "node-1")
		listen   = env("LISTEN_ADDR", ":8080")
		addr     = env("NODE_ADDR", "http://localhost:8080") // địa chỉ để người khác gọi tới node này
		coordURL = env("COORDINATOR_URL", "http://localhost:9000")
		dataDir  = env("DATA_DIR", "./data")
		capacity = envInt64("CAPACITY_BYTES", 1<<30)
		hbEvery  = envDuration("HEARTBEAT_INTERVAL", 5*time.Second)
	)
	log = log.With("node", nodeID)

	store, err := node.NewDiskStore(dataDir)
	if err != nil {
		log.Error("mở thư mục dữ liệu lỗi", "dir", dataDir, "err", err)
		os.Exit(1)
	}
	used, count := store.Stats()
	log.Info("node khởi động", "dir", dataDir, "shard_count", count, "used_bytes", used)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:    listen,
		Handler: (&node.Server{ID: nodeID, Store: store, Log: log}).Routes(),
	}
	go func() {
		log.Info("nghe HTTP", "listen", listen, "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server dừng", "err", err)
			stop()
		}
	}()

	reporter := &node.Reporter{
		CoordinatorURL: coordURL,
		NodeID:         nodeID,
		Addr:           addr,
		Capacity:       capacity,
		Interval:       hbEvery,
		Store:          store,
		Log:            log,
	}
	if err := reporter.Register(ctx); err == nil {
		go reporter.Run(ctx)
	}

	<-ctx.Done()
	log.Info("đang tắt node")
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

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
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
