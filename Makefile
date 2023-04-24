# cleans your local docker instance of containers, images & volumes.  This will remove everything, including items not related to this kaskada project
docker/clean:
	docker container prune -f
	docker image prune -af
	docker volume prune -f
	docker system prune -af

ent/clean:
	find wren/ent/ -type f ! -name 'generate.go' ! -name '.gitignore' ! -regex '.*/schema/.*' -delete

ent/generate: ent/clean
	cd wren && go generate ./ent

# starts a postgres container, runs the create migration tool, then stops the postgres container
ent/create-migrations:
	docker run --rm --name migrate_postgres --detach -p 5433:5432 --env POSTGRES_PASSWORD=mpostgres123 postgres:14.3-alpine
	sleep 5
	cd wren && go run -mod=mod db/main.go
	docker stop migrate_postgres

# fixes the migration checksum file (atlas.sum) after any manual changes are applied to the migration files
ent/fix-checksum:
	docker run --rm -v "$(shell pwd)/wren/db:/db" arigaio/atlas:0.8.0 migrate hash --dir file://db/migrations

# starts a postgres container, applies all the current migrations, runs the schema inspect tool, then stops the postgres container
ent/update-schema:
	docker run --rm --name migrate_postgres --detach -p 5433:5432 --env POSTGRES_PASSWORD=mpostgres123 postgres:14.3-alpine
	sleep 5
	docker run --rm -v "$(shell pwd)/wren/db/migrations:/migrations" --network host migrate/migrate -path=/migrations/ -database postgres://postgres:mpostgres123@0.0.0.0:5433/postgres?sslmode=disable up
	docker run --rm --network host arigaio/atlas:0.8.0 schema inspect -u "postgres://postgres:mpostgres123@0.0.0.0:5433/postgres?sslmode=disable" > wren/db/schema.hcl
	docker stop migrate_postgres

# removes all previously generated protobuf code from the repo
proto/clean:
	@ find gen/proto/go -type f ! -name '*.pb.ent.go' ! -name '.gitignore' ! -name 'go.*' -delete
	@ echo 'checks = ["inherit", "-ST1012"]' > gen/proto/go/staticcheck.conf

# use this varible with `$(call buf_docker_fn,<path>,<additional_docker_flags>)`
# where `<path>` is the desired path from the root of the repo for the buf call
# and `<additional_docker_flags>` are optional additional docker flags to use
buf_docker_fn = docker run --rm --network host --workdir /workspace/proto ${1} \
    --volume "$(shell pwd):/workspace" \
	--volume "${HOME}/.cache:/root/.cache" \
	bufbuild/buf:1.17.0

proto/fmt:
	@ $(call buf_docker_fn) format -w

proto/lint:
	@ $(call buf_docker_fn) lint

# generates the protobuf libraries
.PHONY: proto/kaskada
proto/kaskada:
	@ $(call buf_docker_fn) generate

# regenerates all the protobuf libraries and docs outputted by wren
proto/generate: proto/clean proto/kaskada
	@ echo done!

test/int/docker-up:
	docker compose -f ./tests/integration/docker-compose.yml up --build --remove-orphans

test/int/docker-up-s3:
	docker compose -f ./tests/integration/docker-compose.yml -f ./tests/integration/docker-compose.s3.yml up --build --remove-orphans

test/int/docker-up-postgres:
	docker compose -f ./tests/integration/docker-compose.yml -f ./tests/integration/docker-compose.postgres.yml up --build --remove-orphans

test/int/docker-up-postgres-s3:
	docker compose -f ./tests/integration/docker-compose.yml -f ./tests/integration/docker-compose.postgres.yml -f ./tests/integration/docker-compose.s3.yml up --build --remove-orphans

test/int/run-api:
	cd tests/integration/api && LOCAL=true go run github.com/onsi/ginkgo/v2/ginkgo -v ./...

test/int/run-api-s3:
	cd tests/integration/api && LOCAL=true OBJECT_STORE_TYPE=s3 OBJECT_STORE_PATH=/data go run github.com/onsi/ginkgo/v2/ginkgo -v ./...

test/int/run-api-postgres:
	cd tests/integration/api && LOCAL=true DB_DIALECT="postgres" go run github.com/onsi/ginkgo/v2/ginkgo -v ./...

test/int/run-api-postgres-s3:
	cd tests/integration/api && LOCAL=true DB_DIALECT="postgres" OBJECT_STORE_TYPE=s3 OBJECT_STORE_PATH=/data go run github.com/onsi/ginkgo/v2/ginkgo -v ./...

####
## CI related targets
####
ci/integration/tests/docker-compose-up:
	export DOCKER_BUILDKIT=1 
	docker compose -f ./tests/integration/docker-compose-ci-integration.yml up --build --detach

ci/integration/tests/docker-compose-down:
	export DOCKER_BUILDKIT=1 
	docker compose -f ./tests/integration/docker-compose-ci-integration.yml down

ci/integration/tests/run/api: test/int/run-api


wren/build:
	cp NOTICE wren/
	cd wren && go build main.go

wren/lint:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

wren/run:
	cp NOTICE wren/
	cd wren && go run main.go

wren/test:
	cp NOTICE wren/
	cd wren && go test ./...

.PHONY: sparrow/run sparrow/run-release
sparrow/run:
	cargo run -p sparrow-main serve

sparrow/run-release:
	cargo run --release -p sparrow-main serve

docker/debug: wren/build
	cargo build -p sparrow-main
	mkdir -p release
	cp main release/wren
	cp target/debug/sparrow-main release
	DOCKER_BUILDKIT=1 docker build -f Dockerfile.release .
	rm -fr release

docker/release: wren/build
	cargo build --release -p sparrow-main
	mkdir -p release
	cp main release/wren
	cp target/debug/sparrow-main release
	DOCKER_BUILDKIT=1 docker build -f Dockerfile.release .
	rm -fr release

python/setup:
	cd clients/python && poetry install

python/test:
	cd clients/python && poetry run poe test

python/build:
	cd clients/python && poetry build

python/install: python/build
	pip install clients/python/dist/*.whl;
	