package ec

import (
	"bytes"
	"math/rand"
	"testing"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// Bỏ ngẫu nhiên đúng m shard thì vẫn phải khôi phục nguyên vẹn segment.
func TestEncodeDecodeMissingUpToM(t *testing.T) {
	const (
		k = 4
		m = 2
	)
	c, err := New(k, m)
	if err != nil {
		t.Fatal(err)
	}

	for _, size := range []int{1, 100, 4 << 20, (4 << 20) - 3} {
		segment := randomBytes(size)
		shards, err := c.Encode(segment)
		if err != nil {
			t.Fatalf("size=%d: encode: %v", size, err)
		}
		if len(shards) != k+m {
			t.Fatalf("size=%d: có %d shard, mong đợi %d", size, len(shards), k+m)
		}

		// Bỏ m shard ngẫu nhiên.
		missing := rand.Perm(k + m)[:m]
		for _, i := range missing {
			shards[i] = nil
		}

		got, err := c.Decode(shards, size)
		if err != nil {
			t.Fatalf("size=%d: decode: %v", size, err)
		}
		if !bytes.Equal(got, segment) {
			t.Fatalf("size=%d: nội dung khôi phục khác bản gốc", size)
		}
	}
}

// Thiếu quá m shard thì phải báo lỗi chứ không trả dữ liệu sai.
func TestDecodeFailsWhenTooFewShards(t *testing.T) {
	c, _ := New(4, 2)
	segment := randomBytes(1 << 20)
	shards, err := c.Encode(segment)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1, 2} { // mất 3 shard > m=2
		shards[i] = nil
	}
	if _, err := c.Decode(shards, len(segment)); err == nil {
		t.Fatal("mong đợi lỗi khi còn dưới k shard, nhưng decode thành công")
	}
}

// Shard tái tạo phải giống hệt shard gốc (repair dựa vào tính chất này).
func TestReconstructRestoresIdenticalShards(t *testing.T) {
	c, _ := New(4, 2)
	segment := randomBytes(3 << 20)
	original, err := c.Encode(segment)
	if err != nil {
		t.Fatal(err)
	}

	damaged := make([][]byte, len(original))
	copy(damaged, original)
	damaged[1], damaged[5] = nil, nil

	if err := c.Reconstruct(damaged); err != nil {
		t.Fatal(err)
	}
	for i := range original {
		if !bytes.Equal(original[i], damaged[i]) {
			t.Fatalf("shard %d tái tạo khác bản gốc", i)
		}
		if Hash(original[i]) != Hash(damaged[i]) {
			t.Fatalf("shard %d: hash khác nhau", i)
		}
	}
}
