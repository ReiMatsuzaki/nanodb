BIN=./target/debug/nanodb

run_debug:
	cargo build
	rm ./nano.db; RUST_LOG=debug,my_crate=info ${BIN}
	# RUST_LOG=debug ${BIN}