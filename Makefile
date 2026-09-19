# SnailMQ Makefile — 统一入口

.PHONY: build test vet fmt fmt-fix bench demo help clean

help:
	@echo "SnailMQ Makefile"
	@echo "  make build     go build ./..."
	@echo "  make test      go test -race ./..."
	@echo "  make vet       go vet ./..."
	@echo "  make fmt       gofmt 检查(CI 用,失败即报错)"
	@echo "  make fmt-fix   gofmt -w ."
	@echo "  make bench     内核/端到端压测,输出 benchmark/*.txt"
	@echo "  make demo      go run ./cmd/demo"
	@echo "  make clean     清理产物"
	@echo "  make help      显示帮助"

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	@echo "Checking gofmt..."
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on: $$(gofmt -l .)"; exit 1)

fmt-fix:
	gofmt -w .

bench:
	mkdir -p benchmark
	go test ./benchmark -bench=. -benchmem -run=^$$ 2>&1 | tee benchmark/RESULT.txt

demo:
	go run ./cmd/demo

clean:
	rm -f mqd benchmark/RESULT.txt
