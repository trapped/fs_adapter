.PHONY: all deps test

CRYSTAL ?= crystal

all: deps test

test:
	$(CRYSTAL) spec spec/fs_adapter_spec.cr

deps:
	$(CRYSTAL) deps
