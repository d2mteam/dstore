// Command dsctl là CLI của người dùng: put / get / ls / nodes / rm.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"text/tabwriter"
	"time"

	"dstore-demo/internal/client"
)

const usage = `dsctl - CLI của dstore-demo

Cách dùng:
  dsctl [tùy chọn chung] <lệnh> [tham số]

Lệnh:
  put <đường dẫn file>        Upload file, in ra file_id
  get <file_id> <đầu ra>      Tải file về
  ls                          Danh sách file
  nodes                       Danh sách node và trạng thái
  rm <file_id>                Xóa file
  hash <đường dẫn file>       In SHA-256 của file (để so sánh trước/sau)

Tùy chọn chung:
  -coordinator URL   Địa chỉ coordinator (mặc định http://localhost:9000, hoặc biến COORDINATOR_URL)
  -k N               Số data shard khi upload (mặc định 4)
  -m N               Số parity shard khi upload (mặc định 2)
  -segment BYTES     Kích thước segment khi upload (mặc định 4194304 = 4MB)
  -parallel N        Số request shard song song (mặc định 8)
  -v                 Log chi tiết
`

func main() {
	var (
		coordURL = flag.String("coordinator", env("COORDINATOR_URL", "http://localhost:9000"), "địa chỉ coordinator")
		k        = flag.Int("k", envInt("EC_K", 4), "số data shard")
		m        = flag.Int("m", envInt("EC_M", 2), "số parity shard")
		segment  = flag.Int("segment", envInt("SEGMENT_SIZE", 4<<20), "kích thước segment (byte)")
		parallel = flag.Int("parallel", 8, "số request shard song song")
		verbose  = flag.Bool("v", false, "log chi tiết")
	)
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := client.New(*coordURL, *k, *m, *segment, *parallel, log)
	if err := run(ctx, c, args); err != nil {
		fmt.Fprintln(os.Stderr, "lỗi:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *client.Client, args []string) error {
	switch args[0] {
	case "put":
		if len(args) < 2 {
			return fmt.Errorf("dùng: dsctl put <đường dẫn file>")
		}
		start := time.Now()
		fileID, err := c.Upload(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("file_id: %s  (%.1fs)\n", fileID, time.Since(start).Seconds())
		return nil

	case "get":
		if len(args) < 3 {
			return fmt.Errorf("dùng: dsctl get <file_id> <đầu ra>")
		}
		start := time.Now()
		if err := c.Download(ctx, args[1], args[2]); err != nil {
			return err
		}
		fmt.Printf("đã lưu %s  (%.1fs)\n", args[2], time.Since(start).Seconds())
		return nil

	case "ls":
		files, err := c.Files(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "FILE_ID\tTÊN\tKÍCH THƯỚC\tK/M\tTRẠNG THÁI\tTẠO LÚC")
		for _, f := range files {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\t%s\n",
				f.ID, f.Name, humanBytes(f.Size), f.K, f.M, f.Status, f.CreatedAt.Format(time.TimeOnly))
		}
		return w.Flush()

	case "nodes":
		nodes, err := c.Nodes(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NODE\tĐỊA CHỈ\tTRẠNG THÁI\tSHARD\tĐÃ DÙNG\tHEARTBEAT")
		for _, n := range nodes {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s trước\n",
				n.ID, n.Addr, n.Status, n.ShardCount, humanBytes(n.Used),
				time.Since(n.LastHeartbeat).Truncate(time.Second))
		}
		return w.Flush()

	case "rm":
		if len(args) < 2 {
			return fmt.Errorf("dùng: dsctl rm <file_id>")
		}
		if err := c.Delete(ctx, args[1]); err != nil {
			return err
		}
		fmt.Println("đã xóa", args[1])
		return nil

	case "hash":
		if len(args) < 2 {
			return fmt.Errorf("dùng: dsctl hash <đường dẫn file>")
		}
		sum, err := fileHash(args[1])
		if err != nil {
			return err
		}
		fmt.Println(sum)
		return nil

	default:
		return fmt.Errorf("lệnh không hợp lệ: %s", args[0])
	}
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGT"[exp])
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
