SHELL := /bin/bash

CRYSTAL_CACHE_DIR := $(CURDIR)/.crystal-cache
export CRYSTAL_CACHE_DIR

CRYSTAL ?= crystal
PYTHON ?= python3
SOURCES := $(shell find src/ -type f -name '*.cr')
SPECS := $(shell find spec/ -type f -name '*.cr')
BINARY := out/meshcore-tcp-mux

all: $(BINARY)

$(BINARY): $(SOURCES)
	mkdir -p out
	$(CRYSTAL) build -o $(BINARY) src/main.cr

spec: $(SOURCES) $(SPECS)
	$(CRYSTAL) spec --verbose

format-check:
	$(CRYSTAL) tool format --check src spec

# Requires meshcore==2.3.9.1 and meshcore-cli==1.6.3 in PYTHON's environment.
smoke: $(BINARY)
	$(PYTHON) scripts/check_fake_clients.py --mux-binary $(BINARY)

ci: format-check all spec smoke

clean:
	rm -f $(BINARY)
	# Explicitly write out the .crystal-cache directory name so that any assignment errors don't cause a too-broad rm call.
	rm -rf .crystal-cache

.PHONY: all spec format-check smoke ci clean
