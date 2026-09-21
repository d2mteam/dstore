# Các lệnh hay dùng khi phát triển và khi demo.
COMPOSE := docker compose -f deploy/docker-compose.yml
DOCKER_USER := $(shell id -u):$(shell id -g)
WORK := deploy/work

.PHONY: help build dist test race up down logs ps nodes ls put get demo-file chaos watch sim clean

help: ## In danh sách lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  make %-12s %s\n", $$1, $$2}'

build: ## Build binary cho máy hiện tại, ra thư mục bin/
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/node ./cmd/node
	go build -o bin/dsctl ./cmd/dsctl
	go build -o bin/simulator ./cmd/simulator

dist: ## Build binary tĩnh cho Linux vào bin/linux/ (image Docker dùng bản này)
	CGO_ENABLED=0 GOOS=linux go build -o bin/linux/coordinator ./cmd/coordinator
	CGO_ENABLED=0 GOOS=linux go build -o bin/linux/node ./cmd/node
	CGO_ENABLED=0 GOOS=linux go build -o bin/linux/dsctl ./cmd/dsctl

test: ## Chạy toàn bộ test
	go test ./...

race: ## Chạy test kèm race detector
	go test -race ./...

up: dist ## Bật 1 coordinator + 8 node
	$(COMPOSE) up -d --build

down: ## Tắt cụm và xóa dữ liệu node
	$(COMPOSE) down -v

logs: ## Xem log coordinator (repair, node chết...)
	$(COMPOSE) logs -f coordinator

ps: ## Trạng thái container
	$(COMPOSE) ps

nodes: ## Danh sách node theo góc nhìn coordinator
	@DOCKER_USER=$(DOCKER_USER) $(COMPOSE) run --rm dsctl nodes

ls: ## Danh sách file đã lưu
	@DOCKER_USER=$(DOCKER_USER) $(COMPOSE) run --rm dsctl ls

demo-file: ## Tạo file mẫu 50MB trong deploy/work/
	@mkdir -p $(WORK)
	head -c 50000000 /dev/urandom > $(WORK)/demo.bin
	@echo "đã tạo $(WORK)/demo.bin"
	@sha256sum $(WORK)/demo.bin

put: ## Upload file: make put FILE=demo.bin
	@DOCKER_USER=$(DOCKER_USER) $(COMPOSE) run --rm dsctl put /work/$(FILE)

get: ## Tải file: make get ID=<file_id> OUT=out.bin
	@DOCKER_USER=$(DOCKER_USER) $(COMPOSE) run --rm dsctl get $(ID) /work/$(OUT)
	@sha256sum $(WORK)/$(OUT)

chaos: ## Tắt/bật node ngẫu nhiên trong 2 phút
	./scripts/chaos.sh 120

watch: ## Bảng số segment theo mức độ khỏe, làm mới mỗi 5s
	./scripts/watch-health.sh

sim: ## Mô phỏng nghìn node để chọn k/m và ngưỡng repair
	go run ./cmd/simulator -compare km
	go run ./cmd/simulator -compare threshold
	go run ./cmd/simulator -compare bandwidth

clean: ## Xóa binary và file demo
	rm -rf bin $(WORK)/*.bin
