# rtransfer build shortcuts.
#
#   make            release build for this machine
#   make aarch64    release build for 64-bit ARM Linux (via `cross`)
#   make all        both of the above
#   make check / test / debug / clean
#   make bench-serde  wire-codec comparison (standalone crate in benches/)

# cargo/cross live in ~/.cargo/bin, which PATH may lack (the rustc shim
# breaks without it).
export PATH := $(HOME)/.cargo/bin:$(PATH)

AARCH64_TARGET := aarch64-unknown-linux-gnu
# Some Docker setups break cross's container networking; host networking avoids it.
export CROSS_CONTAINER_OPTS := --network host

.PHONY: release aarch64 all debug check test clean bench-serde

release:
	cargo build --release
	@echo "-> target/release/rtransfer"

aarch64:
	cross build --release --target $(AARCH64_TARGET)
	@echo "-> target/$(AARCH64_TARGET)/release/rtransfer (pass with --serve-binary)"

all: release aarch64

debug:
	cargo build

check:
	cargo check --tests

test:
	cargo test

bench-serde:
	cargo run --release --manifest-path benches/serialization/Cargo.toml

clean:
	cargo clean
