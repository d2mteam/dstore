# Image chỉ đóng gói binary đã build sẵn (xem target `dist` trong Makefile).
#
# Không build Go ngay trong Dockerfile vì bước đó cần tải dependency từ
# proxy.golang.org, mà sandbox mạng của buildkit không phải máy nào cũng ra được
# internet. Build ngoài host bằng module cache sẵn có thì image dựng được offline.
# Nếu máy bạn build có mạng, có thể thay bằng bản multi-stage (xem README).
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY bin/linux/coordinator bin/linux/node bin/linux/dsctl /app/
WORKDIR /app
