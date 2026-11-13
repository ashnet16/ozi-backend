.PHONY: format lint test type-check check


SNAPCHAIN_REPO=https://github.com/farcasterxyz/snapchain.git
PROTO_SRC=./snapchain/proto/definitions
PROTO_DST=./proto

format:
	poetry run black consumers producers rag analytics
	poetry run isort consumers producers rag analytics

lint:
	poetry run flake8 consumers producers rag analytics

test:
	poetry run pytest

type-check:
	poetry run mypy .

check: format lint type-check

.PHONY: setup build start proto clean-proto


create-user:
	sudo groupadd -f ozi && sudo useradd -m -g ozi -s /bin/bash ozi || true

clone-snapchain:
	@if [ ! -d "./snapchain" ]; then \
		echo "Cloning Snapchain..."; \
		git clone --depth 1 $(SNAPCHAIN_REPO); \
	else \
		echo "Snapchain already cloned."; \
	fi

copy-protos: clone-snapchain
	@mkdir -p $(PROTO_DST)
	@cp -r $(PROTO_SRC)/*.proto $(PROTO_DST)/
	@echo "Copied proto files to $(PROTO_DST)"

# The .proto files are compiled inside the poller image (see producers/Dockerfile),
# so no host Python or grpcio-tools is needed.
proto: copy-protos

clean-proto:
	@rm -f $(PROTO_DST)/*_pb2.py $(PROTO_DST)/*_pb2_grpc.py
	@echo "Cleaned compiled proto files."

build: proto
	@docker compose build

start:
	@docker compose up -d

setup: build start
