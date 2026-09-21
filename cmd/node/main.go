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
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/webui"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var (
		nodeID     = env("NODE_ID", "node-1")
		listen     = env("LISTEN_ADDR", ":8080")
		storage    = env("STORAGE", "disk") // disk | s3
		configFile = env("CONFIG_FILE", "") // nơi web UI lưu cấu hình kết nối; rỗng = không lưu
		uiPassword = env("UI_PASSWORD", "")
	)
	log = log.With("node", nodeID)

	// Cấu hình kết nối: env là mặc định, file do web UI ghi (nếu có) đè lên.
	settings := node.Settings{
		CoordinatorURL:    env("COORDINATOR_URL", "http://localhost:9000"),
		Addr:              env("NODE_ADDR", "http://localhost:8080"), // địa chỉ để người khác gọi tới node này
		Capacity:          envInt64("CAPACITY_BYTES", 1<<30),
		HeartbeatInterval: protocol.Duration(envDuration("HEARTBEAT_INTERVAL", 5*time.Second)),
	}.Clean()
	if configFile != "" {
		saved, found, err := node.LoadSettings(configFile, settings)
		if err != nil {
			log.Error("không đọc được file cấu hình", "file", configFile, "err", err)
			os.Exit(1)
		}
		if found {
			settings = saved
			log.Info("dùng cấu hình đã lưu từ web UI thay cho env", "file", configFile)
		}
	}
	if err := settings.Validate(); err != nil {
		log.Error("cấu hình không hợp lệ", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, where, err := openStore(ctx, storage, nodeID, log)
	if err != nil {
		log.Error("không mở được nơi lưu shard", "storage", storage, "err", err)
		os.Exit(1)
	}
	used, count := store.Stats()
	log.Info("node khởi động", "storage", where, "shard_count", count, "used_bytes", used)

	reporter := node.NewReporter(nodeID, settings, store, log)
	admin := &node.Admin{
		ID: nodeID, Store: store, Storage: where, Reporter: reporter,
		ConfigFile: configFile, StartedAt: time.Now(), Log: log,
	}

	// API shard giữ nguyên ở gốc; web UI và API riêng của nó nằm dưới /ui/.
	mux := http.NewServeMux()
	mux.Handle("/", (&node.Server{ID: nodeID, Store: store, Log: log}).Routes())
	mux.Handle("/ui/", webui.Handler(webui.NodePage, admin.Routes(), uiPassword))
	mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))
	if uiPassword == "" {
		log.Warn("chưa đặt UI_PASSWORD: ai vào được mạng này cũng mở được web UI")
	}

	srv := &http.Server{Addr: listen, Handler: mux}
	go func() {
		log.Info("nghe HTTP", "listen", listen, "addr", settings.Addr, "web_ui", "/ui/")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server dừng", "err", err)
			stop()
		}
	}()

	go reporter.Run(ctx)

	<-ctx.Done()
	log.Info("đang tắt node")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

// openStore mở nơi lưu shard theo cấu hình, trả kèm mô tả để hiện trên web UI.
// Với S3 thì chờ tới khi kết nối được, vì trong docker compose node thường
// khởi động trước MinIO.
func openStore(ctx context.Context, kind, nodeID string, log *slog.Logger) (node.ShardStore, string, error) {
	if kind != "s3" {
		dir := env("DATA_DIR", "./data")
		store, err := node.NewDiskStore(dir)
		return store, "disk " + dir, err
	}

	// Khóa truy cập và region theo chuẩn AWS: AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY, AWS_REGION — hoặc IAM role khi chạy trên AWS.
	cfg := node.S3Config{
		Bucket:       env("S3_BUCKET", ""),
		Prefix:       env("S3_PREFIX", "nodes/"+nodeID+"/"),
		Endpoint:     env("S3_ENDPOINT", ""), // để trống khi dùng Amazon S3
		UsePathStyle: env("S3_FORCE_PATH_STYLE", "false") == "true",
		CreateBucket: env("S3_CREATE_BUCKET", "false") == "true",
	}
	where := "s3://" + cfg.Bucket + "/" + cfg.Prefix
	if cfg.Endpoint != "" {
		where += " (" + cfg.Endpoint + ")"
	}
	for attempt := 1; ; attempt++ {
		store, err := node.NewS3Store(ctx, cfg)
		if err == nil {
			log.Info("đã kết nối S3", "endpoint", cfg.Endpoint, "bucket", cfg.Bucket, "prefix", cfg.Prefix)
			return store, where, nil
		}
		log.Warn("chưa kết nối được S3, thử lại", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
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
