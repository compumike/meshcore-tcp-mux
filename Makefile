SHELL=/bin/bash

CRYSTAL_CACHE_DIR := $(CURDIR)/.crystal-cache
export CRYSTAL_CACHE_DIR
        
all: out/meshcore-tcp-mux
        
SOURCES=$(shell find src/ -type f -name '*.cr')
SPECS=$(shell find spec/ -type f -name '*.cr')
BENCH_SOURCES=$(shell find bench/ -type f -name '*.cr')
        
out/meshcore-tcp-mux: $(SOURCES)
	crystal build -o out/meshcore-tcp-mux src/main.cr

spec: $(SOURCES) $(SPECS)
	crystal spec --verbose
        
clean:  
	rm -f out/meshcore-tcp-mux
	# Explicitly write out the .crystal-cache directory name so that any assignment errors don't cause a too-broad rm call.
	rm -rf .crystal-cache

PHONY: all spec clean
