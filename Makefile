all:
	cargo build --features "default static-link portable"
dev:
	cargo build --features "dev"
