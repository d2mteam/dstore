// Package node là data plane: lưu shard trên đĩa theo hash và phục vụ PUT/GET.
package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrHashMismatch: nội dung gửi lên không khớp hash trong URL.
var ErrHashMismatch = errors.New("nội dung không khớp hash")

// DiskStore lưu shard tại <dir>/ab/cd/abcd... (chia thư mục theo 2 cặp ký tự đầu
// của hash để một thư mục không chứa quá nhiều file).
type DiskStore struct {
	dir string

	mu    sync.RWMutex
	used  int64 // tổng byte đang giữ
	count int   // số shard đang giữ
}

func NewDiskStore(dir string) (*DiskStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &DiskStore{dir: dir}
	if err := s.scan(); err != nil {
		return nil, err
	}
	return s, nil
}

// scan đếm lại shard đang có trên đĩa (shard còn nguyên sau khi node restart).
func (s *DiskStore) scan() error {
	return filepath.WalkDir(s.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		s.used += info.Size()
		s.count++
		return nil
	})
}

func (s *DiskStore) path(hash string) string {
	return filepath.Join(s.dir, hash[0:2], hash[2:4], hash)
}

func validHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

// Put ghi shard ra file tạm, kiểm tra hash rồi mới rename vào vị trí chính thức,
// nên trên đĩa không bao giờ có shard ghi dở.
func (s *DiskStore) Put(hash string, r io.Reader) error {
	if !validHash(hash) {
		return fmt.Errorf("hash không hợp lệ: %q", hash)
	}
	final := s.path(hash)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(final), hash+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // không có tác dụng nếu rename đã thành công

	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != hash {
		return fmt.Errorf("%w: nhận %s", ErrHashMismatch, got)
	}

	// Đã có shard này rồi (content addressing: nội dung giống nhau thì file giống nhau).
	if _, err := os.Stat(final); err == nil {
		return nil
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return err
	}

	s.mu.Lock()
	s.used += written
	s.count++
	s.mu.Unlock()
	return nil
}

// Open mở shard để đọc; caller phải Close.
func (s *DiskStore) Open(hash string) (*os.File, int64, error) {
	if !validHash(hash) {
		return nil, 0, fmt.Errorf("hash không hợp lệ: %q", hash)
	}
	f, err := os.Open(s.path(hash))
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

func (s *DiskStore) Has(hash string) bool {
	if !validHash(hash) {
		return false
	}
	_, err := os.Stat(s.path(hash))
	return err == nil
}

func (s *DiskStore) Delete(hash string) error {
	if !validHash(hash) {
		return fmt.Errorf("hash không hợp lệ: %q", hash)
	}
	info, err := os.Stat(s.path(hash))
	if err != nil {
		return err
	}
	if err := os.Remove(s.path(hash)); err != nil {
		return err
	}
	s.mu.Lock()
	s.used -= info.Size()
	s.count--
	s.mu.Unlock()
	return nil
}

// List trả về hash của mọi shard đang giữ (coordinator dùng để đối chiếu khi node sống lại).
func (s *DiskStore) List() ([]string, error) {
	var hashes []string
	err := filepath.WalkDir(s.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}
		if validHash(d.Name()) {
			hashes = append(hashes, d.Name())
		}
		return nil
	})
	return hashes, err
}

// Stats trả về số byte và số shard đang giữ.
func (s *DiskStore) Stats() (used int64, count int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used, s.count
}
