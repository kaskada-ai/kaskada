cli/build:
	cd ../../../clients/cli/ && go build -o ../../tests/integration/cli/kaskada_cli ./main.go

cli/test:
	go run github.com/onsi/ginkgo/v2/ginkgo -v ./...
