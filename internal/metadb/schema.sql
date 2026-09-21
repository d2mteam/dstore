-- Chỉ lưu trạng thái cứng của coordinator: file gồm những segment nào, shard nào
-- nằm trên node nào. Trạng thái sống/chết của node và shard dựng lại từ heartbeat
-- nên không có ở đây (xem internal/coordinator/persist.go).

CREATE TABLE IF NOT EXISTS nodes (
    id        TEXT PRIMARY KEY,
    addr      TEXT   NOT NULL,
    capacity  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id            TEXT PRIMARY KEY,
    name          TEXT        NOT NULL,
    size          BIGINT      NOT NULL,
    segment_size  INT         NOT NULL,
    k             INT         NOT NULL,
    m             INT         NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    file_id    TEXT NOT NULL REFERENCES files (id) ON DELETE CASCADE,
    seg_index  INT  NOT NULL,
    orig_size  INT  NOT NULL, -- cần cho Join cắt phần đệm
    PRIMARY KEY (file_id, seg_index)
);

-- node_id cố ý không có khóa ngoại tới nodes: ghi node lỗi không được phép làm
-- hỏng việc commit file.
CREATE TABLE IF NOT EXISTS shards (
    file_id      TEXT NOT NULL,
    seg_index    INT  NOT NULL,
    shard_index  INT  NOT NULL, -- Reed-Solomon cần biết shard số mấy
    hash         TEXT NOT NULL,
    node_id      TEXT NOT NULL,
    PRIMARY KEY (file_id, seg_index, shard_index),
    FOREIGN KEY (file_id, seg_index) REFERENCES segments (file_id, seg_index) ON DELETE CASCADE
);

-- "Shard nào nằm trên node X" — câu hỏi repair và dọn rác cần trả lời.
CREATE INDEX IF NOT EXISTS shards_node_id ON shards (node_id);

-- Tham số vận hành sửa qua web UI (xem internal/coordinator/settings.go). Chỉ có
-- đúng một dòng; lưu JSON để thêm tham số mới không phải đổi bảng.
CREATE TABLE IF NOT EXISTS settings (
    id          INT PRIMARY KEY CHECK (id = 1),
    data        JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
