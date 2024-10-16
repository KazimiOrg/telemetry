rustserver_path := justfile_directory() + "/rust-server"
dist_path := justfile_directory() + "/dist"

run config_path:
    #!/usr/bin/env bash
    set -exo pipefail
    CONFIG_PATH="$(realpath {{ config_path }})"
    if [ ! -f "${CONFIG_PATH}" ]; then
        echo "Config file does not exist"
        exit 1
    fi
    cd {{ rustserver_path }}
    RUST_LOG=debug RUST_BACKTRACE=1 cargo run -- --config-path ${CONFIG_PATH}

test:
    cd {{ rustserver_path }} && cargo test

build-dist-x86_64 config_path: check-env
    #!/usr/bin/env bash
    set -exo pipefail
    CONFIG_PATH="$(realpath {{ config_path }})"
    if [ ! -f "${CONFIG_PATH}" ]; then
        echo "Config file does not exist"
        exit 1
    fi
    cd {{ rustserver_path }}
    cargo clean && cross build --release --locked --target x86_64-unknown-linux-gnu

    rm -rf {{ dist_path }}
    mkdir -p {{ dist_path }}
    cp {{ rustserver_path }}/target/x86_64-unknown-linux-gnu/release/server {{ dist_path }}/rashitelemetryserver
    cp ${CONFIG_PATH} {{ dist_path }}/config.yaml
    echo "TelemetryServer files packaged in {{ dist_path }}"

check-env:
    #!/usr/bin/env bash
    set -exo pipefail
    which cargo >/dev/null
    which cross >/dev/null
