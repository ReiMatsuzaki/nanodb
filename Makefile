BIN=./target/debug/nanodb

run_debug:
	cargo build
	rm ./*.db; RUST_LOG=debug,my_crate=info ${BIN}

run_info:
	cargo build
	rm ./*.db; RUST_LOG=info,my_crate=info ${BIN}