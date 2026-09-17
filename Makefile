SHELL := /bin/bash

CRYSTAL_CACHE_DIR := $(CURDIR)/.crystal-cache
export CRYSTAL_CACHE_DIR

CRYSTAL ?= crystal
BUILD_FLAGS ?=
PYTHON ?= python3
DOCKER_IMAGE ?= compumike/meshcore-tcp-mux:dev
DOCKER_PLATFORMS ?= linux/amd64,linux/arm64
DOCKER_BUILDER ?=
DOCKER_BUILDER_FLAG = $(if $(DOCKER_BUILDER),--builder $(DOCKER_BUILDER),)
SOURCES := $(shell find src/ -type f -name '*.cr')
SPECS := $(shell find spec/ -type f -name '*.cr')
BINARY := out/meshcore-tcp-mux

all: $(BINARY)

$(BINARY): $(SOURCES)
	mkdir -p out
	$(CRYSTAL) build $(BUILD_FLAGS) -o $(BINARY) src/main.cr

spec: $(SOURCES) $(SPECS)
	$(CRYSTAL) spec --verbose

format-check:
	$(CRYSTAL) tool format --check src spec

# Requires meshcore==2.3.9.1 and meshcore-cli==1.6.3 in PYTHON's environment.
smoke: $(BINARY)
	$(PYTHON) -m unittest discover -s scripts -p 'test_*.py'
	$(PYTHON) scripts/check_fake_clients.py --mux-binary $(BINARY)

ci: format-check all spec smoke

# Builds and tests the host architecture, then loads it into local Docker.
docker-build:
	docker buildx build $(DOCKER_BUILDER_FLAG) --load --tag $(DOCKER_IMAGE) .

# Linux Docker Engine only; uses a temporary bridge and no physical companion.
docker-smoke:
	$(PYTHON) scripts/check_docker.py --image $(DOCKER_IMAGE)

# Explicit publication: the selected builder must support both architectures.
# Both platforms run the Dockerfile's specs and fake-companion smoke test.
docker-push:
	docker buildx build $(DOCKER_BUILDER_FLAG) --platform $(DOCKER_PLATFORMS) --tag $(DOCKER_IMAGE) --push .

clean:
	rm -f $(BINARY)
	# Explicitly write out the .crystal-cache directory name so that any assignment errors don't cause a too-broad rm call.
	rm -rf .crystal-cache

loc:
	@count_lines() { \
		find "$$1" -type f -name "$$2" \
			-exec awk '!/^[[:space:]]*$$/ && !/^[[:space:]]*#/ { count++ } END { print count + 0 }' {} + | \
			awk '{ total += $$1 } END { print total + 0 }'; \
	}; \
	src_lines=$$(count_lines src '*.cr'); \
	spec_lines=$$(count_lines spec '*.cr'); \
	script_lines=$$(count_lines scripts '*.py'); \
	printf 'src (*.cr): %d\nspec (*.cr): %d\nscripts (*.py): %d\ntotal: %d\n' \
		"$$src_lines" "$$spec_lines" "$$script_lines" \
		"$$((src_lines + spec_lines + script_lines))"

.PHONY: all spec format-check smoke ci docker-build docker-smoke docker-push clean loc
