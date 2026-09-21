// Command coordinator là control plane: registry node, metadata file, repair.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/metadb"
	"dstore-demo/internal/pgdb"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
	"dstore-demo/internal/webui"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("svc", "coordinator")

	var (
		listen      = env("LISTEN_ADDR", ":9000")
		dbURL       = env("DATABASE_URL", "")
		healthEvery = envDuration("HEALTH_INTERVAL", 5*time.Second)
		repairEvery = envDuration("REPAIR_INTERVAL", 10*time.Second)
		gcEvery     = envDuration("GC_INTERVAL", 60*time.Second)
		uiPassword  = env("UI_PASSWORD", "")
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	meta, closeMeta, err := openMetaStore(ctx, dbURL, log)
	if err != nil {
		log.Error("không kết nối được DB metadata", "err", err)
		os.Exit(1)
	}
	defer closeMeta()

	metrics := &coordinator.Metrics{}
	store := coordinator.NewStore(meta)
	nodes, files, err := store.Load(ctx)
	if err != nil {
		log.Error("không đọc được metadata", "err", err)
		os.Exit(1)
	}
	log.Info("đã khôi phục metadata", "nodes", nodes, "files", files)

	// Tham số vận hành: env là mặc định, bản đã sửa qua web UI (lưu trong DB) đè lên.
	settings := coordinator.NewSettingsStore(meta, coordinator.Settings{
		HeartbeatTimeout: protocol.Duration(envDuration("HEARTBEAT_TIMEOUT", 20*time.Second)),
		GCGrace:          protocol.Duration(envDuration("GC_GRACE", 2*time.Minute)),
	})
	found, err := settings.Load(ctx)
	if err != nil {
		log.Error("không đọc được settings", "err", err)
		os.Exit(1)
	}
	cur := settings.Get()
	if found {
		log.Info("dùng settings đã lưu từ web UI thay cho env", "settings", cur)
	}
	if cur.RepairPaused {
		log.Warn("repair tự động đang TẠM DỪNG (đặt từ web UI): shard mất sẽ không được tái tạo")
	}
	if cur.GCPaused {
		log.Warn("GC tự động đang TẠM DỪNG (đặt từ web UI)")
	}

	placer := coordinator.NewPlacer(store)
	shards := shardclient.New(30 * time.Second)

	health := &coordinator.HealthMonitor{
		Store: store, Shards: shards, Settings: settings,
		Interval: healthEvery, Metrics: metrics, Log: log,
	}
	repairer := &coordinator.Repairer{
		Store: store, Placer: placer, Shards: shards, Settings: settings,
		Interval: repairEvery, Metrics: metrics, Log: log,
	}
	collector := &coordinator.Collector{
		Store: store, Shards: shards, Settings: settings,
		Interval: gcEvery, Metrics: metrics, Log: log,
	}
	api := &coordinator.Server{
		Store: store, Placer: placer, Health: health, Shards: shards, Metrics: metrics, Log: log,
	}
	admin := &coordinator.Admin{
		Store: store, Meta: meta, Settings: settings,
		Repairer: repairer, Collector: collector, Metrics: metrics, Log: log,
		Startup: coordinator.StartupInfo{
			Listen:         listen,
			Database:       describeDB(dbURL),
			HealthInterval: protocol.Duration(healthEvery),
			RepairInterval: protocol.Duration(repairEvery),
			GCInterval:     protocol.Duration(gcEvery),
			StartedAt:      time.Now(),
		},
	}

	go health.Run(ctx)
	go repairer.Run(ctx)
	go collector.Run(ctx)

	// API control plane giữ nguyên ở gốc; web UI và API riêng của nó nằm dưới /ui/.
	mux := http.NewServeMux()
	mux.Handle("/", api.Routes())
	mux.Handle("/ui/", webui.Handler(webui.CoordinatorPage, admin.Routes(), uiPassword))
	mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))
	if uiPassword == "" {
		log.Warn("chưa đặt UI_PASSWORD: ai vào được mạng này cũng mở được web UI")
	}

	srv := &http.Server{Addr: listen, Handler: mux}
	go func() {
		log.Info("coordinator nghe HTTP", "listen", listen, "web_ui", "/ui/",
			"heartbeat_timeout", cur.HeartbeatTimeout, "repair_interval", repairEvery,
			"gc_interval", gcEvery, "gc_grace", cur.GCGrace)
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

// openMetaStore kết nối PostgreSQL, chờ tới khi DB sẵn sàng (trong docker compose
// coordinator thường khởi động trước Postgres). Không đặt DATABASE_URL thì giữ
// metadata trong RAM như bản đầu của demo.
func openMetaStore(ctx context.Context, url string, log *slog.Logger) (coordinator.MetaStore, func(), error) {
	if url == "" {
		log.Warn("chưa đặt DATABASE_URL: metadata chỉ nằm trong RAM, restart là mất")
		return coordinator.NewMemoryMeta(), func() {}, nil
	}
	cfg := pgdb.Config{URL: url, IAMAuth: env("DB_IAM_AUTH", "false") == "true"}
	if env("DB_AUTO_MIGRATE", "true") == "true" {
		cfg.Schema = metadb.Schema
	}
	for attempt := 1; ; attempt++ {
		pool, err := pgdb.Open(ctx, cfg)
		if err == nil {
			log.Info("đã kết nối PostgreSQL", "iam_auth", cfg.IAMAuth, "auto_migrate", cfg.Schema != "")
			return metadb.New(pool), pool.Close, nil
		}
		log.Warn("chưa kết nối được PostgreSQL, thử lại", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// describeDB bỏ mật khẩu và tham số khỏi chuỗi kết nối để hiện trên web UI.
func describeDB(dsn string) string {
	if dsn == "" {
		return "memory (restart là mất metadata)"
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "postgres" // dạng "host=... password=...": không đoán được phần nào an toàn để hiện
	}
	return "postgres://" + u.Host + u.Path
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
