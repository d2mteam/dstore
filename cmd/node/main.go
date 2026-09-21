// Command node là data plane: cất shard (đĩa local hoặc MinIO) và gửi heartbeat
// cho coordinator.
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
		storage  = env("STORAGE", "disk") // disk | s3
		capacity = envInt64("CAPACITY_BYTES", 1<<30)
		hbEvery  = envDuration("HEARTBEAT_INTERVAL", 5*time.Second)
	)
	log = log.With("node", nodeID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := openStore(ctx, storage, nodeID, log)
	if err != nil {
		log.Error("không mở được nơi lưu shard", "storage", storage, "err", err)
		os.Exit(1)
	}
	used, count := store.Stats()
	log.Info("node khởi động", "storage", storage, "shard_count", count, "used_bytes", used)

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

// openStore mở nơi lưu shard theo cấu hình. Với MinIO thì chờ tới khi nó sẵn
// sàng, vì trong docker compose node thường khởi động trước MinIO.
func openStore(ctx context.Context, kind, nodeID string, log *slog.Logger) (node.ShardStore, error) {
	if kind != "s3" {
		return node.NewDiskStore(env("DATA_DIR", "./data"))
	}

	cfg := node.S3Config{
		Endpoint:  env("S3_ENDPOINT", "localhost:9000"),
		AccessKey: env("S3_ACCESS_KEY", "minioadmin"),
		SecretKey: env("S3_SECRET_KEY", "minioadmin"),
		Bucket:    env("S3_BUCKET", nodeID), // mặc định mỗi node một bucket trùng tên node
		UseSSL:    env("S3_USE_SSL", "false") == "true",
	}
	for attempt := 1; ; attempt++ {
		store, err := node.NewS3Store(ctx, cfg)
		if err == nil {
			log.Info("đã kết nối MinIO", "endpoint", cfg.Endpoint, "bucket", cfg.Bucket)
			return store, nil
		}
		log.Warn("chưa kết nối được MinIO, thử lại", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
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
