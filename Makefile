.PHONY: format lint test type-check check


SNAPCHAIN_REPO=https://github.com/farcasterxyz/snapchain.git
PROTO_SRC=./snapchain/proto/definitions
PROTO_DST=./proto

format:
	poetry run black consumers producers rag analytics
	poetry run isort consumers producers rag analytics

lint:
	poetry run flake8 consumers producers rag analytics

# Runs every test in tests/ without Docker. Creates .venv (Python 3.11, via uv),
# installs tests/requirements.txt and compiles the protos the first time.
# Pass pytest options with ARGS, e.g. `make test ARGS="-k search -x"`.
TEST_VENV=.venv
TEST_PYTHON=$(TEST_VENV)/bin/python

test: test-setup
	$(TEST_PYTHON) -m pytest $(ARGS)

test-setup: $(TEST_PYTHON) proto-local
	@uv pip install --quiet --python $(TEST_PYTHON) -r tests/requirements.txt

$(TEST_PYTHON):
	uv venv --python 3.11 $(TEST_VENV)

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

# Compile the protos on the host as well, for the tests. Generated files land in
# ./proto (git-ignored); the poller image still compiles its own copy.
.PHONY: proto-local test-setup
proto-local: $(TEST_PYTHON)
	@if [ ! -f $(PROTO_DST)/rpc_pb2_grpc.py ]; then \
		$(MAKE) proto && \
		uv pip install --quiet --python $(TEST_PYTHON) grpcio-tools==1.71.0 && \
		$(TEST_PYTHON) -m grpc_tools.protoc -I$(PROTO_DST) --python_out=$(PROTO_DST) \
			--grpc_python_out=$(PROTO_DST) $(PROTO_DST)/*.proto; \
	fi

clean-proto:
	@rm -f $(PROTO_DST)/*_pb2.py $(PROTO_DST)/*_pb2_grpc.py
	@echo "Cleaned compiled proto files."

build: proto
	@docker compose build

start:
	@docker compose up -d

setup: build start

# Local Snapchain devnet (4 nodes, built from the Snapchain clone). Node 1 serves
# gRPC on localhost:3383 and HTTP on localhost:3381, where the poller looks by default.
.PHONY: snapchain-up snapchain-down snapchain-logs

snapchain-up: clone-snapchain
	@docker compose -f snapchain/docker-compose.yml up -d --build

snapchain-down:
	@docker compose -f snapchain/docker-compose.yml down

snapchain-logs:
	@docker compose -f snapchain/docker-compose.yml logs -f node1
