
.PHONY: build run debug test k6 k6-smoke

build:
	cargo build --release

run:
	cargo run --release

debug:
	cargo run

test:
	cargo +stable test

k6:
	docker run --rm -i \
		--network host \
		-v "$$(pwd):/work" \
		-w /work \
		-e BASE_URL="$${BASE_URL:-http://172.17.0.1:3001}" \
		-e VUS="$${VUS:-10}" \
		-e DURATION="$${DURATION:-30s}" \
		-e RUN_ID="$${RUN_ID:-local}" \
		grafana/k6 run k6/webhooks-load.js

k6-smoke:
	docker run --rm -i \
		--network host \
		-v "$$(pwd):/work" \
		-w /work \
		-e BASE_URL="$${BASE_URL:-http://172.17.0.1:3001}" \
		-e VUS="100" \
		-e DURATION="60s" \
		-e RUN_ID="$${RUN_ID:-smoke}" \
		grafana/k6 run k6/webhooks-load.js

k6-smoke-10k:
	docker run --rm -i \
		--network host \
		-v "$$(pwd):/work" \
		-w /work \
		-e BASE_URL="$${BASE_URL:-http://172.17.0.1:3001}" \
		-e VUS="10000" \
		-e DURATION="60s" \
		-e RUN_ID="$${RUN_ID:-smoke-10k}" \
		grafana/k6 run k6/webhooks-load.js