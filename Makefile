.PHONY: all deps

all: deps test

test:
	LIBRARY_PATH="/opt/crystal/embedded/lib" \
	~/crystal/bin/crystal spec spec/fs_adapter_spec.cr

deps:
	LIBRARY_PATH="/opt/crystal/embedded/lib" \
	~/crystal/bin/crystal deps
