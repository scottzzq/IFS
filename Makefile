all: clean
	cargo build --features "default static-link portable"
dev:
	cargo build --features "dev"
clean:
	sh ./stop.sh
