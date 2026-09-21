-- Bảng của gateway: file nào thuộc tenant nào. Tách khỏi các bảng của
-- coordinator vì hai dịch vụ có thể dùng hai DB khác nhau.
CREATE TABLE IF NOT EXISTS file_owners (
    file_id       TEXT PRIMARY KEY,
    tenant        TEXT NOT NULL,
    name          TEXT NOT NULL,
    content_type  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS file_owners_tenant ON file_owners (tenant);
