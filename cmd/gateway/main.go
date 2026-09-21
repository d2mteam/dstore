// Command gateway là API công khai của hệ thống: ứng dụng bên ngoài PUT/GET file
// bằng HTTP thường, không cần biết tới erasure coding, coordinator hay node.
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

	"dstore-demo/internal/client"
	"dstore-demo/internal/gateway"
	"dstore-demo/internal/pgdb"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("svc", "gateway")

	var (
		listen        = env("LISTEN_ADDR", ":8000")
		coordURL      = env("COORDINATOR_URL", "http://localhost:9000")
		apiKeys       = env("API_KEYS", "demo-key:demo")
		k             = envInt("EC_K", 4)
		m             = envInt("EC_M", 2)
		segmentSize   = envInt("SEGMENT_SIZE", 4<<20)
		parallel      = envInt("SHARD_PARALLEL", 8)
		maxUpload     = envInt64("MAX_UPLOAD_BYTES", 512<<20)
		maxConcurrent = envInt("MAX_CONCURRENT", 32)
	)

	keys, err := gateway.ParseKeys(apiKeys)
	if err != nil {
		log.Error("API_KEYS không hợp lệ", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	owners, closeOwners, err := openOwners(ctx, env("DATABASE_URL", ""), log)
	if err != nil {
		log.Error("không kết nối được DB", "err", err)
		os.Exit(1)
	}
	defer closeOwners()

	storage := client.New(coordURL, k, m, segmentSize, parallel, log)
	srv := &http.Server{
		Addr:    listen,
		Handler: gateway.New(storage, keys, owners, maxUpload, maxConcurrent, log).Routes(),
		// Upload file lớn có thể lâu, nên chỉ đặt hạn cho lúc đọc header.
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("gateway nghe HTTP", "listen", listen, "coordinator", coordURL,
			"k", k, "m", m, "max_upload_bytes", maxUpload, "max_concurrent", maxConcurrent,
			"tenants", len(keys))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server dừng", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("đang tắt gateway")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

// openOwners kết nối PostgreSQL, chờ tới khi DB sẵn sàng. Không đặt
// DATABASE_URL thì giữ bảng chủ sở hữu trong RAM — chỉ đúng khi chạy một bản.
func openOwners(ctx context.Context, url string, log *slog.Logger) (gateway.Owners, func(), error) {
	if url == "" {
		log.Warn("chưa đặt DATABASE_URL: chủ sở hữu file chỉ nằm trong RAM, " +
			"restart là mất và không chạy được nhiều bản gateway")
		return gateway.NewMemoryOwners(), func() {}, nil
	}
	cfg := pgdb.Config{URL: url, IAMAuth: env("DB_IAM_AUTH", "false") == "true"}
	if env("DB_AUTO_MIGRATE", "true") == "true" {
		cfg.Schema = gateway.OwnersSchema
	}
	for attempt := 1; ; attempt++ {
		pool, err := pgdb.Open(ctx, cfg)
		if err == nil {
			log.Info("đã kết nối PostgreSQL", "iam_auth", cfg.IAMAuth, "auto_migrate", cfg.Schema != "")
			return gateway.NewPostgresOwners(pool), pool.Close, nil
		}
		log.Warn("chưa kết nối được PostgreSQL, thử lại", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
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

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
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
