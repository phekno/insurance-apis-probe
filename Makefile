.PHONY: build test vet run docker-build docker-run

build:
	go build -o bin/probe ./cmd/probe

test:
	go test ./...

vet:
	go vet ./...

run:
	go run ./cmd/probe

docker-build:
	docker buildx build --platform linux/amd64 --load -t insurance-apis-probe:dev .

docker-run: docker-build
	docker run --rm -p 8080:8080 insurance-apis-probe:dev
